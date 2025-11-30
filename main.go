package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
	"encoding/json"
	"sd_tarea3/common" // Asume que el directorio es sd_tarea3
)

// Configuración de Nodos
// IDs: 1 (10.10.31.76), 2 (10.10.31.77), 3 (10.10.31.78)
var NodeAddresses = map[int]string{
	1: "10.10.31.76:8081",
	2: "10.10.31.77:8082",
	3: "10.10.31.78:8083",
}

// ServerNode representa una instancia del nodo (Primario o Secundario).
type ServerNode struct {
	ID             int
	Address        string
	State          *common.ReplicatedState // Módulo de persistencia
	CurrentPrimary int
	IsPrimary      bool
	StatusMutex    sync.RWMutex
	StopMonitoring chan bool // Canal para detener el monitoreo
}

// NewServerNode inicializa un nuevo nodo.
func NewServerNode(id int) *ServerNode {
	node := &ServerNode{
		ID:             id,
		Address:        NodeAddresses[id],
		State:          &common.ReplicatedState{},
		CurrentPrimary: -1, // Inicialmente desconocido
		IsPrimary:      false,
		StopMonitoring: make(chan bool),
	}
	// Módulo de persistencia: Cargar el estado
	if err := node.State.Load(id); err != nil {
		log.Fatalf("Error al cargar estado para el nodo %d: %v", id, err)
	}
	return node
}

// Módulo de Operaciones: HandleClientRequest (RPC)
// Este método es llamado por el cliente o por otros nodos.
func (n *ServerNode) HandleClientRequest(req *common.Event, reply *string) error {
	n.StatusMutex.RLock()
	isPrimary := n.IsPrimary
	primaryID := n.CurrentPrimary
	n.StatusMutex.RUnlock()

	// CORRECCIÓN: Aceptamos nil O un evento con Op "READ" como solicitud de lectura
	if req == nil || req.Op == "READ" {
		// Solicitud de lectura (Revisar inventario)
		return n.handleReadRequest(reply)
	}

	if !isPrimary {
		// Informar al cliente quién es el primario
		*reply = fmt.Sprintf("SECONDARY:%d", primaryID)
		return nil
	}

	// Solicitud de escritura (Modificar inventario)
	return n.handleWriteRequest(req, reply)
}

func (n *ServerNode) handleReadRequest(reply *string) error {
	// Las solicitudes de lectura no generan nuevos eventos[cite: 78].
	// Devuelve el estado actual del inventario.
	n.State.Mu.RLock()
	defer n.State.Mu.RUnlock()

	inventoryJSON, err := json.MarshalIndent(n.State.Inventory, "", "  ")
	if err != nil {
		return fmt.Errorf("error al serializar inventario: %w", err)
	}

	*reply = fmt.Sprintf("INVENTORY:%s\nSequence: %d", string(inventoryJSON), n.State.SequenceNumber)
	return nil
}

El cliente se queda "pegado" porque estás sufriendo un Deadlock Distribuido (Bloqueo Mutuo) o un bloqueo por operaciones de red lentas mientras mantienes los recursos cerrados.

El problema técnico: Tu función handleWriteRequest en el Nodo 3 bloquea el acceso al nodo (StatusMutex y State.Mu) y mantiene ese bloqueo mientras intenta comunicarse con los otros nodos (Replicación).

El Nodo 3 bloquea sus recursos.

Intenta contactar al Nodo 1 (Falla rápido: connection refused).

Intenta contactar al Nodo 2.

Si el Nodo 2 está intentando contactar al Nodo 3 al mismo tiempo (por ejemplo, enviando una Elección o Monitoreo), el Nodo 3 no puede responder porque tiene el candado puesto.

Resultado: El Nodo 3 espera al Nodo 2, y el Nodo 2 espera al Nodo 3. El cliente espera eternamente.

🛠️ La Solución: Liberar el Candado antes de Replicar
Debemos modificar handleWriteRequest en main.go para que aplique los cambios locales, libere los candados (Unlock) y luego intente replicar a los otros nodos. Esto permite que el Nodo 3 siga respondiendo a otras peticiones (como "CheckPrimary" o "Election") mientras espera que la replicación termine.

Reemplaza la función handleWriteRequest en main.go (en todas las MVs) con esta versión optimizada:

Go

// main.go

func (n *ServerNode) handleWriteRequest(req *common.Event, reply *string) error {
	// 1. BLOQUEO: Solo para actualizar el estado local
	n.StatusMutex.Lock()
	n.State.Mu.Lock()

	// Asignar número de secuencia y aplicar localmente
	req.Seq = n.State.SequenceNumber + 1
	fmt.Printf("🔄 Primary (%d) recibe escritura. Asigna Seq: %d. Replicando a secundarios...\n", n.ID, req.Seq)
	
	n.State.ApplyEvent(*req) 
	if err := n.State.Persist(n.ID); err != nil {
		log.Printf("Error al persistir estado local después de evento %d: %v", req.Seq, err)
	}

	// 2. DESBLOQUEO: Liberamos los recursos ANTES de salir a la red
	// Esto evita deadlocks si los secundarios nos están contactando al mismo tiempo.
	n.State.Mu.Unlock()
	n.StatusMutex.Unlock()

	// 3. REPLICACIÓN: Ahora es seguro hacer llamadas RPC lentas
	successCount := 0
	for id, addr := range NodeAddresses {
		if id != n.ID {
			// Añadimos un pequeño timeout manual o manejo de error simple
			if err := n.replicateEvent(addr, *req); err == nil {
				successCount++
			} else {
				// Solo loguear, no detenerse
				log.Printf("⚠️ Error al replicar evento %d a nodo %d (%s): %v", req.Seq, id, addr, err)
			}
		}
	}

	// Validación de éxito (Consistencia eventual para esta tarea)
	if successCount == len(NodeAddresses)-1 {
		fmt.Printf("✅ Evento %d replicado exitosamente a todos los secundarios. Total: %d.\n", req.Seq, successCount)
		*reply = fmt.Sprintf("SUCCESS: Evento %d procesado y replicado.", req.Seq)
	} else {
		fmt.Printf("❌ Advertencia: Evento %d procesado localmente, pero falló replicación a %d nodos.\n", req.Seq, (len(NodeAddresses)-1)-successCount)
		*reply = fmt.Sprintf("WARNING: Evento %d procesado localmente, falló replicación a algunos nodos.", req.Seq)
	}

	return nil
}

// replicateEvent llama al RPC del secundario para aplicar un evento.
func (n *ServerNode) replicateEvent(secondaryAddr string, event common.Event) error {
	client, err := rpc.Dial("tcp", secondaryAddr)
	if err != nil {
		return err
	}
	defer client.Close()

	var reply string
	err = client.Call("ServerNode.ReceiveReplication", event, &reply)
	if err != nil {
		return err
	}
	return nil
}

// Módulo de Operaciones: ReceiveReplication (RPC)
// Método llamado por el primario para replicar un evento.
func (n *ServerNode) ReceiveReplication(event common.Event, reply *string) error {
	n.StatusMutex.RLock()
	isPrimary := n.IsPrimary
	n.StatusMutex.RUnlock()

	if isPrimary {
		// El primario no debe recibir replicación de sí mismo.
		*reply = "IGNORAR"
		return nil
	}

	n.State.ApplyEvent(event)
	if err := n.State.Persist(n.ID); err != nil {
		log.Printf("Error al persistir estado local después de replicación: %v", err)
	}
	*reply = "OK"
	return nil
}

// Módulo de Monitoreo: CheckPrimary (RPC)
// Método llamado por los secundarios para vigilar al primario[cite: 53].
func (n *ServerNode) CheckPrimary(ignored bool, reply *string) error {
	n.StatusMutex.RLock()
	if n.IsPrimary {
		*reply = "ACK" // El primario responde con un ACK [cite: 40]
	} else {
		*reply = "NACK"
	}
	n.StatusMutex.RUnlock()
	return nil
}

// Módulo de Sincronización: GetState (RPC)
// Método llamado por un nodo que se reintegra para obtener el estado actual[cite: 63, 64].
func (n *ServerNode) GetState(ignored bool, reply *common.ReplicatedState) error {
	n.StatusMutex.RLock()
	if !n.IsPrimary {
		n.StatusMutex.RUnlock()
		return fmt.Errorf("no soy el primario, no puedo entregar el estado completo")
	}
	n.StatusMutex.RUnlock()

	n.State.Mu.RLock()
	defer n.State.Mu.RUnlock()

	// Retorna una copia del estado persistente
	*reply = *n.State
	return nil
}

// 2. Mecanismo de detección de fallos [cite: 52]
func (n *ServerNode) StartMonitoring() {
	ticker := time.NewTicker(2 * time.Second) // Monitoreo periódico
	defer ticker.Stop()

	for {
		select {
		case <-n.StopMonitoring:
			fmt.Println("🛑 Deteniendo monitoreo del primario.")
			return
		case <-ticker.C:
			n.StatusMutex.RLock()
			primaryID := n.CurrentPrimary
			n.StatusMutex.RUnlock()

			if primaryID == n.ID || primaryID == -1 {
				// No monitorea si es primario o si aún no hay primario.
				continue
			}

			// Intentar contactar al primario
			primaryAddr := NodeAddresses[primaryID]
			client, err := rpc.Dial("tcp", primaryAddr)
			if err != nil {
				// No se pudo conectar: Fallo asumido [cite: 54]
				fmt.Printf("🔴 Fallo detectado: Nodo primario (%d) en %s no responde. Iniciando elección de líder...\n", primaryID, primaryAddr)
				go n.StartElection()
				continue
			}
			defer client.Close()

			var reply string
			// CheckPrimary es el ACK periódico [cite: 40, 53]
			err = client.Call("ServerNode.CheckPrimary", true, &reply)
			if err != nil || reply != "ACK" {
				// La conexión falló o el nodo no se identificó como primario
				fmt.Printf("🔴 Fallo detectado: Nodo primario (%d) en %s falló CheckPrimary. Iniciando elección de líder...\n", primaryID, primaryAddr)
				go n.StartElection()
				continue
			}

			// fmt.Printf("🟢 ACK recibido de Primario (%d).\n", primaryID)
		}
	}
}

// 1. Elección de líder (Algoritmo del matón) [cite: 47]
func (n *ServerNode) StartElection() {
	n.StatusMutex.Lock()
	if n.IsPrimary {
		n.StatusMutex.Unlock()
		return
	}
	n.StatusMutex.Unlock()

	fmt.Printf("📢 Nodo %d: Iniciando elección...\n", n.ID)

	// Variable para saber si encontramos a alguien digno que SÍ tomó el mando
	leaderFound := false

	for id, addr := range NodeAddresses {
		if id > n.ID {
			// Intentar contactar al nodo mayor
			if n.sendElection(addr) {
				fmt.Printf("   -> Nodo más alto (%d) respondió 'OK'. Esperando coordinación...\n", id)
				
				// Esperamos un momento para ver si cumple su promesa
				time.Sleep(3 * time.Second)

				// Verificamos si ya hay un nuevo líder
				n.StatusMutex.RLock()
				primaryID := n.CurrentPrimary
				n.StatusMutex.RUnlock()

				if primaryID != -1 && primaryID != n.ID {
					// ¡El nodo mayor cumplió! Terminamos.
					leaderFound = true
					fmt.Println("   -> Coordinación recibida exitosamente.")
					return 
				}

				// Si llegamos aquí, el nodo mayor respondió pero NO tomó el mando.
				fmt.Printf("⚠️ El nodo %d respondió pero falló en coordinar. Lo ignoro y continúo.\n", id)
				// NO hacemos return. Seguimos buscando o nos autoproclamamos.
			}
		}
	}

	// Si terminamos el bucle y nadie tomó el mando (o nadie respondió), me autoproclamo.
	if !leaderFound {
		n.becomePrimary()
	}
}
// sendElection envía un mensaje de elección. Retorna true si recibe respuesta (OK).
func (n *ServerNode) sendElection(addr string) bool {
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return false // No responde
	}
	defer client.Close()

	var reply string
	// ElectionRequest es un mensaje para iniciar el proceso de Matón
	err = client.Call("ServerNode.ElectionRequest", n.ID, &reply)
	if err != nil {
		return false // Falló la llamada RPC
	}

	return reply == "OK"
}

func (n *ServerNode) ElectionRequest(callerID int, reply *string) error {
	fmt.Printf("   <- Recibido mensaje 'Election' de Nodo %d. Respondiendo 'OK'.\n", callerID)
	*reply = "OK"

	n.StatusMutex.RLock()
	isPrimary := n.IsPrimary
	n.StatusMutex.RUnlock()

	if isPrimary {
		// CASO CRÍTICO: Soy el primario y alguien menor inició una elección (tal vez pensó que morí).
		// No inicio elección, simplemente les recuerdo a todos quién manda.
		fmt.Printf("   ⚠️ Recibí elección de %d siendo yo Primario. Reafirmando autoridad...\n", callerID)
		go n.broadcastCoordinator()
	} else {
		// Algoritmo estándar: Si soy mayor que el que llama, tomo el relevo.
		if callerID < n.ID {
			go n.StartElection()
		}
	}

	return nil
}

// CoordinatorMessage (RPC): Recibe un mensaje de "Coordinator" del nuevo primario.
func (n *ServerNode) CoordinatorMessage(newPrimaryID int, reply *string) error {
	n.StatusMutex.Lock()
	// No usamos defer aquí para poder liberar el lock antes de llamar al monitoreo
	
	if n.IsPrimary {
		if newPrimaryID > n.ID {
			n.IsPrimary = false
			n.CurrentPrimary = newPrimaryID
			fmt.Printf("📣 Nuevo primario: Nodo %d. Yo soy secundario.\n", newPrimaryID)
			n.StatusMutex.Unlock() // IMPORTANTE: Liberar antes de iniciar monitoreo

			// Reiniciar monitoreo en una goroutine
			go n.restartMonitoring()
		} else {
			n.StatusMutex.Unlock()
			fmt.Printf("⚠️ Coordinador (%d) ignorado, mi ID (%d) es más alto.\n", newPrimaryID, n.ID)
		}
	} else {
		n.CurrentPrimary = newPrimaryID
		fmt.Printf("📣 Nuevo primario: Nodo %d. Yo soy secundario.\n", newPrimaryID)
		n.StatusMutex.Unlock() // IMPORTANTE: Liberar antes de iniciar monitoreo
		
		// Reiniciar monitoreo en una goroutine
		go n.restartMonitoring()
	}

	*reply = "ACK"
	return nil
}

// Función auxiliar para reiniciar el monitoreo de forma segura
func (n *ServerNode) restartMonitoring() {
	// Intentar detener el monitoreo anterior si existe, sin bloquear
	select {
	case n.StopMonitoring <- true:
	default:
		// No había monitoreo corriendo o nadie escuchaba, continuamos
	}
	// Iniciar el nuevo bucle de monitoreo
	n.StartMonitoring()
}

// broadcastCoordinator envía un mensaje de "Coordinator" a todos los demás nodos.
func (n *ServerNode) broadcastCoordinator() {
	for id, addr := range NodeAddresses {
		if id != n.ID {
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				log.Printf("⚠️ Error al enviar mensaje 'Coordinator' a nodo %d (%s): %v", id, addr, err)
				continue
			}
			defer client.Close()

			var reply string
			client.Call("ServerNode.CoordinatorMessage", n.ID, &reply)
		}
	}
	fmt.Println("   -> Mensaje 'Coordinator' enviado a todos los nodos.")
}

// Lógica para que el nodo se convierta en primario.
func (n *ServerNode) becomePrimary() {
	n.StatusMutex.Lock() // Asegurar exclusión mutua al cambiar estado
	n.IsPrimary = true
	n.CurrentPrimary = n.ID
	n.StatusMutex.Unlock()

	// Detener el monitoreo de forma no bloqueante
	select {
	case n.StopMonitoring <- true:
	default:
	}

	fmt.Printf("👑 Nodo %d es el nuevo Primario.\n", n.ID)
	fmt.Printf("====================================================\n")
	fmt.Printf("LOG: ELECCIÓN COMPLETADA: PRIMARIO ES NODO %d\n", n.ID)
	fmt.Printf("====================================================\n")

	// Enviar mensajes de coordinador en una goroutine para no bloquear
	go n.broadcastCoordinator()
}

// 5. Reintegración: Lógica de recuperación
func (n *ServerNode) Reintegrate() {
	fmt.Println("🚀 Iniciando proceso de reintegración...")

	// 1. Descubrir quién es el primario actual
	primaryID := n.discoverPrimary()
	if primaryID == -1 {
		fmt.Println("❌ No se pudo encontrar al primario. Intentando iniciar elección...")
		n.CurrentPrimary = -1 // Limpiar estado de primario conocido
		n.StartElection()     // Intentar iniciar la elección
		// Después de la elección, la función terminará y el nodo reiniciará monitoreo/servicio.
		return
	}

	// 2. Contactar al primario para obtener el estado actual
	primaryAddr := NodeAddresses[primaryID]
	client, err := rpc.Dial("tcp", primaryAddr)
	if err != nil {
		fmt.Printf("❌ Error al conectar con el primario %d (%s) para sincronización: %v\n", primaryID, primaryAddr, err)
		return
	}
	defer client.Close()

	var newState common.ReplicatedState
	err = client.Call("ServerNode.GetState", true, &newState)
	if err != nil {
		fmt.Printf("❌ Error al obtener el estado del primario %d: %v\n", primaryID, err)
		return
	}

	// 3. Aplicar el estado recuperado
	n.State.Mu.Lock()
	n.State.Inventory = newState.Inventory
	n.State.SequenceNumber = newState.SequenceNumber
	n.State.EventLog = newState.EventLog // Sobrescribir su estado previo [cite: 65]
	n.State.Mu.Unlock()

	// 4. Persistir el nuevo estado
	if err := n.State.Persist(n.ID); err != nil {
		log.Printf("Error al persistir el estado reintegrado: %v", err)
	}

	n.StatusMutex.Lock()
	n.CurrentPrimary = primaryID
	n.StatusMutex.Unlock()

	fmt.Printf("✅ Reintegración exitosa. Nuevo estado con secuencia %d.\n", n.State.SequenceNumber)
	// 7. Logs de ejecución [cite: 83]
	fmt.Printf("====================================================\n")
	fmt.Printf("LOG: REINTEGRACIÓN: NODO %d SINCRONIZADO CON PRIMARIO %d\n", n.ID, primaryID)
	fmt.Printf("====================================================\n")
}

// Descubre el primario consultando a los nodos conocidos.
func (n *ServerNode) discoverPrimary() int {
	// Intentar contactar a todos para encontrar al primario.
	for id, addr := range NodeAddresses {
		if id != n.ID {
			client, err := rpc.Dial("tcp", addr)
			if err == nil {
				defer client.Close()
				var reply string
				// Un nodo secundario responderá con el ID del primario[cite: 94].
				err = client.Call("ServerNode.HandleClientRequest", nil, &reply)
				if err == nil {
					// El formato de respuesta es "SECONDARY:ID" si es secundario.
					if len(reply) > 10 && reply[:10] == "SECONDARY:" {
						primaryID, _ := strconv.Atoi(reply[10:])
						return primaryID
					} else if len(reply) > 10 && reply[:9] == "INVENTORY" {
						// Si responde con el inventario, es porque es el primario.
						return id
					}
				}
			}
		}
	}
	return -1 // Primario no encontrado
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Uso: go run main.go <node_id> [primary_on_start]")
		os.Exit(1)
	}

	nodeID, err := strconv.Atoi(os.Args[1])
	if err != nil || NodeAddresses[nodeID] == "" {
		log.Fatalf("ID de nodo inválido: %s", os.Args[1])
	}

	node := NewServerNode(nodeID)
	address := NodeAddresses[nodeID]

	// Módulo de coordinación/monitoreo: Lógica de inicio
	if len(os.Args) == 3 && os.Args[2] == "primary_on_start" {
		// Inicio forzado como primario (solo para el primer nodo al levantar el sistema).
		node.StatusMutex.Lock()
		node.becomePrimary()
		node.StatusMutex.Unlock()
	} else if node.State.SequenceNumber > 0 {
		// Asume que si ya tiene estado, es una reintegración
		node.Reintegrate()
	} else {
		// Es un inicio normal. Intentar encontrar un líder o iniciar elección.
		go node.StartElection()
	}

	// Iniciar monitoreo si no es el primario actual.
	if !node.IsPrimary {
		go node.StartMonitoring()
	}

	// Iniciar servidor RPC
	rpc.Register(node)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Error al escuchar en %s: %v", address, err)
	}
	defer listener.Close()

	fmt.Printf("🚀 Nodo %d ejecutándose en %s...\n", node.ID, address)

	// Manejo de señales para una salida limpia (siMulando fail-stop)
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigc
		fmt.Printf("\n🛑 Señal de terminación recibida. Guardando estado final...\n")
		// 7. Logs de ejecución [cite: 84]
		fmt.Printf("====================================================\n")
		fmt.Printf("LOG: ESTADO FINAL NODO %d\n", node.ID)
		fmt.Printf("Secuencia final: %d\n", node.State.SequenceNumber)
		fmt.Printf("====================================================\n")
		if err := node.State.Persist(node.ID); err != nil {
			log.Printf("Error al guardar estado al salir: %v", err)
		}
		os.Exit(0)
	}()

	// Servir peticiones RPC
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go rpc.ServeConn(conn)
	}
}