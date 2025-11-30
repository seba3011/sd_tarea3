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
	// Las solicitudes de lectura no generan nuevos eventos.
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


func (n *ServerNode) handleWriteRequest(req *common.Event, reply *string) error {
    // bloqueo local para escritura segura
	n.StatusMutex.Lock()
	n.State.Mu.Lock()
	req.Seq = n.State.SequenceNumber + 1
	fmt.Printf("🔄 Primary (%d) recibe escritura. Asigna Seq: %d. Replicando...\n", n.ID, req.Seq)
	
	n.State.ApplyEvent(*req)
	n.State.Persist(n.ID)

    // desbloqueo critico antes de replicar para no congelar
	n.State.Mu.Unlock()
	n.StatusMutex.Unlock()

	successCount := 0
	for id, addr := range NodeAddresses {
		if id != n.ID {
			if err := n.replicateEvent(addr, *req); err == nil {
				successCount++
			} else {
				fmt.Printf("⚠️ Falló rep a %d: %v\n", id, err)
			}
		}
	}

	if successCount == len(NodeAddresses)-1 {
		*reply = fmt.Sprintf("SUCCESS: Evento %d procesado y replicado.", req.Seq)
	} else {
		*reply = fmt.Sprintf("WARNING: Evento %d procesado, pero falló replicación a algunos nodos.", req.Seq)
	}

	return nil
}

func (n *ServerNode) replicateEvent(secondaryAddr string, event common.Event) error {
    // timeout rapido de conexion tcp
	conn, err := net.DialTimeout("tcp", secondaryAddr, 1*time.Second)
	if err != nil {
		return fmt.Errorf("timeout conexión: %v", err)
	}
	
	client := rpc.NewClient(conn)
	defer client.Close()

	var reply string

    // llamada asincrona para evitar bloqueo infinito
	call := client.Go("ServerNode.ReceiveReplication", event, &reply, nil)

	select {
	case <-call.Done:
		return call.Error
	case <-time.After(1 * time.Second):
		return fmt.Errorf("timeout RPC: nodo lento o colgado")
	}
}


func (n *ServerNode) ReceiveReplication(event common.Event, reply *string) error {
	n.StatusMutex.RLock()
	isPrimary := n.IsPrimary
	n.StatusMutex.RUnlock()

	if isPrimary {
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

func (n *ServerNode) CheckPrimary(ignored bool, reply *string) error {
	n.StatusMutex.RLock()
	if n.IsPrimary {
		*reply = "ACK"
	} else {
		*reply = "NACK"
	}
	n.StatusMutex.RUnlock()
	return nil
}

func (n *ServerNode) GetState(ignored bool, reply *common.ReplicatedState) error {
	n.StatusMutex.RLock()
	if !n.IsPrimary {
		n.StatusMutex.RUnlock()
		return fmt.Errorf("no soy el primario, no puedo entregar el estado completo")
	}
	n.StatusMutex.RUnlock()

	n.State.Mu.RLock()
	defer n.State.Mu.RUnlock()

	*reply = *n.State
	return nil
}


func (n *ServerNode) StartMonitoring() {
    // monitoreo constante del lider para detectar fallos
	ticker := time.NewTicker(2 * time.Second) 
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
				continue
			}

			primaryAddr := NodeAddresses[primaryID]
			client, err := rpc.Dial("tcp", primaryAddr)
			if err != nil {
				fmt.Printf("🔴 Fallo detectado: Nodo primario (%d) en %s no responde. Iniciando elección de líder...\n", primaryID, primaryAddr)
				go n.StartElection()
				continue
			}
			defer client.Close()

			var reply string
			
			err = client.Call("ServerNode.CheckPrimary", true, &reply)
			if err != nil || reply != "ACK" {
				fmt.Printf("🔴 Fallo detectado: Nodo primario (%d) en %s falló CheckPrimary. Iniciando elección de líder...\n", primaryID, primaryAddr)
				go n.StartElection()
				continue
			}

		}
	}
}


func (n *ServerNode) StartElection() {
	n.StatusMutex.Lock()
	if n.IsPrimary {
		n.StatusMutex.Unlock()
		return
	}
	n.StatusMutex.Unlock()

	fmt.Printf("📢 Nodo %d: Iniciando elección...\n", n.ID)
    // algoritmo de eleccion bully buscando ids mayores
	leaderFound := false

	for id, addr := range NodeAddresses {
		if id > n.ID {
			if n.sendElection(addr) {
				fmt.Printf("   -> Nodo más alto (%d) respondió 'OK'. Esperando coordinación...\n", id)
				time.Sleep(3 * time.Second)

				n.StatusMutex.RLock()
				primaryID := n.CurrentPrimary
				n.StatusMutex.RUnlock()

				if primaryID != -1 && primaryID != n.ID {
					leaderFound = true
					fmt.Println("   -> Coordinación recibida exitosamente.")
					return 
				}

				fmt.Printf("⚠️ El nodo %d respondió pero falló en coordinar. Lo ignoro y continúo.\n", id)
			}
		}
	}

	if !leaderFound {
		n.becomePrimary()
	}
}

func (n *ServerNode) sendElection(addr string) bool {
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return false 
	}
	defer client.Close()

	var reply string

	err = client.Call("ServerNode.ElectionRequest", n.ID, &reply)
	if err != nil {
		return false 
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
        // reafirma autoridad si ya es lider ante una eleccion
		fmt.Printf("   ⚠️ Recibí elección de %d siendo yo Primario. Reafirmando autoridad...\n", callerID)
		go n.broadcastCoordinator()
	} else {
		if callerID < n.ID {
			go n.StartElection()
		}
	}

	return nil
}


func (n *ServerNode) CoordinatorMessage(newPrimaryID int, reply *string) error {
	n.StatusMutex.Lock()
    // actualizacion del nuevo lider en nodos secundarios
	if n.IsPrimary {
		if newPrimaryID > n.ID {
			n.IsPrimary = false
			n.CurrentPrimary = newPrimaryID
			fmt.Printf("📣 Nuevo primario: Nodo %d. Yo soy secundario.\n", newPrimaryID)
			n.StatusMutex.Unlock() 
			go n.restartMonitoring()
		} else {
			n.StatusMutex.Unlock()
			fmt.Printf("⚠️ Coordinador (%d) ignorado, mi ID (%d) es más alto.\n", newPrimaryID, n.ID)
		}
	} else {
		n.CurrentPrimary = newPrimaryID
		fmt.Printf("📣 Nuevo primario: Nodo %d. Yo soy secundario.\n", newPrimaryID)
		n.StatusMutex.Unlock() 
		
		go n.restartMonitoring()
	}

	*reply = "ACK"
	return nil
}

func (n *ServerNode) restartMonitoring() {
	select {
	case n.StopMonitoring <- true:
	default:
	}
	n.StartMonitoring()
}

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


func (n *ServerNode) becomePrimary() {
	n.StatusMutex.Lock() 
	n.IsPrimary = true
	n.CurrentPrimary = n.ID
	n.StatusMutex.Unlock()

	select {
	case n.StopMonitoring <- true:
	default:
	}

	fmt.Printf("👑 Nodo %d es el nuevo Primario.\n", n.ID)
	fmt.Printf("====================================================\n")
	fmt.Printf("LOG: ELECCIÓN COMPLETADA: PRIMARIO ES NODO %d\n", n.ID)
	fmt.Printf("====================================================\n")

	go n.broadcastCoordinator()
}

func (n *ServerNode) Reintegrate() {
	fmt.Println("🚀 Iniciando proceso de reintegración...")

	primaryID := n.discoverPrimary()
	if primaryID == -1 {
		fmt.Println("❌ No se pudo encontrar al primario. Intentando iniciar elección...")
		n.CurrentPrimary = -1 
		n.StartElection()    
		return
	}

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

	n.State.Mu.Lock()
	n.State.Inventory = newState.Inventory
	n.State.SequenceNumber = newState.SequenceNumber
	n.State.EventLog = newState.EventLog 
	n.State.Mu.Unlock()

	if err := n.State.Persist(n.ID); err != nil {
		log.Printf("Error al persistir el estado reintegrado: %v", err)
	}

	n.StatusMutex.Lock()
	n.CurrentPrimary = primaryID
	n.StatusMutex.Unlock()

	fmt.Printf("✅ Reintegración exitosa. Nuevo estado con secuencia %d.\n", n.State.SequenceNumber)
	fmt.Printf("====================================================\n")
	fmt.Printf("LOG: REINTEGRACIÓN: NODO %d SINCRONIZADO CON PRIMARIO %d\n", n.ID, primaryID)
	fmt.Printf("====================================================\n")
}


func (n *ServerNode) discoverPrimary() int {
	for id, addr := range NodeAddresses {
		if id != n.ID {
			client, err := rpc.Dial("tcp", addr)
			if err == nil {
				defer client.Close()
				var reply string
				err = client.Call("ServerNode.HandleClientRequest", nil, &reply)
				if err == nil {
					if len(reply) > 10 && reply[:10] == "SECONDARY:" {
						primaryID, _ := strconv.Atoi(reply[10:])
						return primaryID
					} else if len(reply) > 10 && reply[:9] == "INVENTORY" {
						return id
					}
				}
			}
		}
	}
	return -1 
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

	if len(os.Args) == 3 && os.Args[2] == "primary_on_start" {
		node.StatusMutex.Lock()
		node.becomePrimary()
		node.StatusMutex.Unlock()
	} else if node.State.SequenceNumber > 0 {
		node.Reintegrate()
	} else {
		go node.StartElection()
	}

	if !node.IsPrimary {
		go node.StartMonitoring()
	}

	rpc.Register(node)
	_, portStr, _ := net.SplitHostPort(address)
    // escucha en cualquier ip del puerto para evitar errores de red
	listener, err := net.Listen("tcp", ":"+portStr)
	if err != nil {
		log.Fatalf("Error al escuchar en %s: %v", address, err)
	}
	defer listener.Close()

	fmt.Printf("🚀 Nodo %d ejecutándose en %s...\n", node.ID, address)

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigc
		fmt.Printf("\n🛑 Señal de terminación recibida. Guardando estado final...\n")
		fmt.Printf("====================================================\n")
		fmt.Printf("LOG: ESTADO FINAL NODO %d\n", node.ID)
		fmt.Printf("Secuencia final: %d\n", node.State.SequenceNumber)
		fmt.Printf("====================================================\n")
        // guarda la persistencia de datos al cerrar el programa
		if err := node.State.Persist(node.ID); err != nil {
			log.Printf("Error al guardar estado al salir: %v", err)
		}
		os.Exit(0)
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go rpc.ServeConn(conn)
	}
}