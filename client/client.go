package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"

	"sd_tarea3/common" // Asume que el directorio es sd_tarea3
)

// Configuración de Nodos (debe ser consistente con main.go)
var NodeAddresses = map[int]string{
	1: "10.10.31.76:8081",
	2: "10.10.31.77:8082",
	3: "10.10.31.78:8083",
}

var knownPrimaryID = -1

func main() {
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("=======================================")
	fmt.Println(" Cliente de Sistema de Inventario Distribuido")
	fmt.Println("=======================================")

	for {
		fmt.Println("\n--- MENÚ DEL CLIENTE ---")
		fmt.Println("1. Revisar inventario")
		fmt.Println("2. Modificar inventario")
		fmt.Println("3. Salir")
		fmt.Print("Ingrese opción: ")

		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		switch input {
		case "1":
			reviewInventory()
		case "2":
			modifyInventory(reader)
		case "3":
			fmt.Println("Saliendo del cliente.")
			return
		default:
			fmt.Println("Opción inválida.")
		}
	}
}

// 9. Descubrimiento del líder por parte del cliente [cite: 90]
func discoverPrimary() (int, string) {
	// Intentar con el primario conocido primero.
	if knownPrimaryID != -1 && NodeAddresses[knownPrimaryID] != "" {
		if checkNode(knownPrimaryID) == knownPrimaryID {
			return knownPrimaryID, NodeAddresses[knownPrimaryID]
		}
	}

	// Contactar a un nodo de la lista de nodos conocidos[cite: 93].
	for id, addr := range NodeAddresses {
		primaryID := checkNode(id)
		if primaryID == id {
			// El nodo contactado es el primario.
			knownPrimaryID = primaryID
			return primaryID, addr
		} else if primaryID != -1 {
			// El nodo contactado es secundario, e indicó el primario actual[cite: 94].
			knownPrimaryID = primaryID
			return primaryID, NodeAddresses[primaryID]
		}
	}

	// Si ninguno de los 3 nodos responde[cite: 96].
	fmt.Println("❌ No es posible contactar con el sistema: Ningún nodo responde.")
	return -1, ""
}

// checkNode intenta conectar a un nodo. Retorna su ID si es el primario, o el ID
// del primario que le indicó (si es secundario), o -1 si falla.
func checkNode(nodeID int) int {
	addr := NodeAddresses[nodeID]
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		// fmt.Printf("⚠️ Nodo %d en %s no responde.\n", nodeID, addr) // Opcional: comentar para menos ruido
		return -1 // Nodo no responde
	}
	defer client.Close()

	var reply string
	
	// CORRECCIÓN: Enviar evento READ en lugar de nil
	readEvent := common.Event{Op: "READ"}
	err = client.Call("ServerNode.HandleClientRequest", &readEvent, &reply)
	
	if err != nil {
		fmt.Printf("⚠️ Error RPC con Nodo %d: %v\n", nodeID, err)
		return -1
	}

	// El formato es "SECONDARY:ID" si es secundario [cite: 94]
	if len(reply) > 10 && reply[:10] == "SECONDARY:" {
		primaryID, _ := strconv.Atoi(reply[10:])
		return primaryID
	} else if len(reply) > 9 && reply[:9] == "INVENTORY" {
		// El primario responde con el inventario
		return nodeID
	}

	return -1
}

// 8. Funcionalidades del cliente: Revisar inventario [cite: 87]
func reviewInventory() {
	primaryID, primaryAddr := discoverPrimary()
	if primaryID == -1 {
		return
	}
	fmt.Printf("🔍 Contactando al Primario (Nodo %d) en %s para la lectura.\n", primaryID, primaryAddr)

	client, err := rpc.Dial("tcp", primaryAddr)
	if err != nil {
		fmt.Println("❌ Error al conectar con el primario para leer:", err)
		knownPrimaryID = -1 
		return
	}
	defer client.Close()

	var reply string
	
	// CORRECCIÓN: Enviar evento READ en lugar de nil
	readEvent := common.Event{Op: "READ"}
	err = client.Call("ServerNode.HandleClientRequest", &readEvent, &reply)
	
	if err != nil {
		fmt.Println("❌ Error en la lectura del inventario:", err)
		knownPrimaryID = -1
		return
	}

	if len(reply) > 9 && reply[:9] == "INVENTORY" {
		fmt.Println("\n--- INVENTARIO ACTUAL ---")
		// Desplegar la lista de ítems [cite: 87]
		inventoryJSON := reply[10:strings.LastIndex(reply, "\n")] // Quitar "INVENTORY:" y "Sequence: X"
		sequenceLine := reply[strings.LastIndex(reply, "\n")+1:]

		var inventory map[string]common.Item
		if err := json.Unmarshal([]byte(inventoryJSON), &inventory); err != nil {
			fmt.Println("Error al parsear inventario:", err)
			return
		}

		for name, item := range inventory {
			fmt.Printf("  - %s: Cantidad=%d, Precio=%d\n", name, item.Quantity, item.Price)
		}
		fmt.Println(sequenceLine)
		fmt.Println("------------------------")
	} else {
		fmt.Println("❌ Respuesta inesperada del primario:", reply)
	}
}

// 8. Funcionalidades del cliente: Modificar inventario [cite: 88]
func modifyInventory(reader *bufio.Reader) {
	// --- (Parte 1: Obtener datos del usuario igual que antes) ---
	fmt.Println("\n--- MODIFICAR INVENTARIO ---")
	fmt.Println("a. Modificar cantidad")
	fmt.Println("b. Modificar precio")
	fmt.Print("Ingrese opción (a/b): ")
	
	opType, _ := reader.ReadString('\n')
	opType = strings.ToLower(strings.TrimSpace(opType))

	fmt.Print("Ingrese nombre del ítem a modificar: ")
	itemName, _ := reader.ReadString('\n')
	itemName = strings.TrimSpace(strings.ToUpper(itemName))

	fmt.Print("Ingrese el nuevo valor (cantidad/precio): ")
	valueStr, _ := reader.ReadString('\n')
	newValue, err := strconv.Atoi(strings.TrimSpace(valueStr))
	if err != nil {
		fmt.Println("❌ Valor ingresado debe ser un número entero.")
		return
	}

	var op string
	switch opType {
	case "a":
		op = common.OpSetQuantity
	case "b":
		op = common.OpSetPrice
	default:
		fmt.Println("❌ Opción de modificación inválida. Use 'a' o 'b'.")
		return
	}

	event := common.Event{Op: op, Item: itemName, Value: newValue, Seq: 0}

	// --- (Parte 2: Bucle con Timeout de Red) ---
	for {
		primaryID, primaryAddr := discoverPrimary()
		if primaryID == -1 {
			return
		}

		if knownPrimaryID != primaryID {
			fmt.Printf("✏️ Contactando al Nodo %d en %s...\n", primaryID, primaryAddr)
		}

		// CORRECCIÓN CLAVE: Usar DialTimeout
		// Si el nodo está pegado y no acepta conexión en 2 segundos, cancelamos.
		conn, err := net.DialTimeout("tcp", primaryAddr, 2*time.Second)
		if err != nil {
			fmt.Printf("⚠️ Timeout/Error contactando Nodo %d. Buscando otro...\n", primaryID)
			knownPrimaryID = -1 // Forzar nuevo descubrimiento
			continue
		}

		client := rpc.NewClient(conn)
		var reply string
		
		// Llamada RPC
		err = client.Call("ServerNode.HandleClientRequest", &event, &reply)
		client.Close() 

		if err != nil {
			fmt.Println("❌ Error RPC:", err)
			knownPrimaryID = -1
			continue
		}

		// Manejar Redirección
		if len(reply) > 10 && reply[:10] == "SECONDARY:" {
			newPrimaryID, _ := strconv.Atoi(reply[10:])
			// Solo imprimir si cambiamos de objetivo
			if knownPrimaryID != newPrimaryID {
				fmt.Printf("🔄 El Nodo %d dice que el líder es %d. Redirigiendo...\n", primaryID, newPrimaryID)
			}
			knownPrimaryID = newPrimaryID
			continue
		}

		fmt.Println("\n--- RESULTADO ---")
		fmt.Println(reply)
		fmt.Println("-----------------")
		break
	}
}