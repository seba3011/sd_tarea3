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

	"sd_tarea3/common" 
)

var NodeAddresses = map[int]string{
    // direccion ip real de cada nodo
	1: "10.10.31.76:8081",
	2: "10.10.31.77:8082",
	3: "10.10.31.78:8083",
}

var knownPrimaryID = 3 // asume nodo 3 como lider al inicio

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

func discoverPrimary() (int, string) {
	if knownPrimaryID != -1 && NodeAddresses[knownPrimaryID] != "" {
		if checkNode(knownPrimaryID) == knownPrimaryID {
			return knownPrimaryID, NodeAddresses[knownPrimaryID]
		}
	}

	for id, addr := range NodeAddresses {
		primaryID := checkNode(id)
		if primaryID == id {

			knownPrimaryID = primaryID
			return primaryID, addr
		} else if primaryID != -1 {
			knownPrimaryID = primaryID
			return primaryID, NodeAddresses[primaryID]
		}
	}

	fmt.Println("❌ No es posible contactar con el sistema: Ningún nodo responde.")
	return -1, ""
}

func checkNode(nodeID int) int {
	addr := NodeAddresses[nodeID]

    // timeout rapido para saltar nodos muertos
	conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
	if err != nil {

		return -1
	}
	
	client := rpc.NewClient(conn)
	defer client.Close()

	var reply string
	readEvent := common.Event{Op: "READ"}
	err = client.Call("ServerNode.HandleClientRequest", &readEvent, &reply)
	
	if err != nil {
		return -1
	}

	if len(reply) > 10 && reply[:10] == "SECONDARY:" {
		primaryID, _ := strconv.Atoi(reply[10:])
		return primaryID
	} else if len(reply) > 9 && reply[:9] == "INVENTORY" {
		return nodeID
	}

	return -1
}

func reviewInventory() {
	primaryID, primaryAddr := discoverPrimary()
	if primaryID == -1 {
		return
	}
	fmt.Printf("🔍 Contactando al Primario (Nodo %d) en %s para la lectura.\n", primaryID, primaryAddr)

	conn, err := net.DialTimeout("tcp", primaryAddr, 2*time.Second)
	if err != nil {
		fmt.Println("❌ Error al conectar con el primario:", err)
		knownPrimaryID = -1 
		return
	}

	client := rpc.NewClient(conn)
	defer client.Close()

	var reply string
	readEvent := common.Event{Op: "READ"}
	err = client.Call("ServerNode.HandleClientRequest", &readEvent, &reply)
	
	if err != nil {
		fmt.Println("❌ Error en la lectura del inventario:", err)
		knownPrimaryID = -1
		return
	}

	if len(reply) > 9 && reply[:9] == "INVENTORY" {
		fmt.Println("\n--- INVENTARIO ACTUAL ---")
		inventoryJSON := reply[10:strings.LastIndex(reply, "\n")] 
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
	} else if len(reply) > 10 && reply[:10] == "SECONDARY:" {
		newID, _ := strconv.Atoi(reply[10:])
		fmt.Printf("🔄 El nodo %d indica que el líder es %d. Reintente.\n", primaryID, newID)
		knownPrimaryID = newID
	} else {
		fmt.Println("❌ Respuesta inesperada del primario:", reply)
	}
}

func modifyInventory(reader *bufio.Reader) {

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
	for { // reintenta operacion si hay fallos
		primaryID, primaryAddr := discoverPrimary()
		if primaryID == -1 {
			fmt.Println("⏳ Esperando sistema...")
			time.Sleep(2 * time.Second)
			continue
		}

		if knownPrimaryID != primaryID {
			fmt.Printf("✏️ Contactando al Nodo %d en %s...\n", primaryID, primaryAddr)
		}
        // conecta con timeout para no congelar
		conn, err := net.DialTimeout("tcp", primaryAddr, 2*time.Second)
		if err != nil {
			fmt.Printf("⚠️ No se pudo conectar al Nodo %d. Reintentando...\n", primaryID)
			knownPrimaryID = -1 
			time.Sleep(2 * time.Second)
			continue
		}

		client := rpc.NewClient(conn)
		var reply string
        // llamada asincrona para evitar deadlock
		call := client.Go("ServerNode.HandleClientRequest", &event, &reply, nil)
		select {
			case <-call.Done:
				err = call.Error
			case <-time.After(12 * time.Second): // espera extendida para replicacion lenta
				err = fmt.Errorf("timeout: el servidor aceptó la conexión pero tardó demasiado en replicar")
		}
		
		client.Close()

		if err != nil {
			fmt.Printf("⚠️ Error RPC con Nodo %d: %v\n", primaryID, err)
			knownPrimaryID = -1
			time.Sleep(2 * time.Second) 
			continue
		}
		if len(reply) > 10 && reply[:10] == "SECONDARY:" {
			newPrimaryID, _ := strconv.Atoi(reply[10:])
			if newPrimaryID == -1 {
                // espera si el sistema esta eligiendo lider
				fmt.Println("⏳ El nodo contactado está en votación (Líder desconocido). Esperando 2s...")
				time.Sleep(2 * time.Second)
				knownPrimaryID = -1 
				continue
			}
			if knownPrimaryID != newPrimaryID {
				fmt.Printf("🔄 El Nodo %d dice que el líder es %d. Redirigiendo...\n", primaryID, newPrimaryID)
			}
			knownPrimaryID = newPrimaryID // actualiza quien es el nuevo lider
			continue
		}

		fmt.Println("\n--- RESULTADO ---")
		fmt.Println(reply)
		fmt.Println("-----------------")
		break
	}
}