package common

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

const (
	OpSetQuantity = "SET_QTY"
	OpSetPrice    = "SET_PRICE"
	InventoryFile = "nodo_%d.json" 
)

type Item struct {
	Quantity int `json:"quantity"`
	Price    int `json:"price"`
}

type Event struct {
	Seq   int    `json:"seq"`
	Op    string `json:"op"`
	Item  string `json:"item"`
	Value int    `json:"value"`
}

// estructura del estado replicado con mutex
type ReplicatedState struct {
	SequenceNumber int             `json:"sequence_number"`
	Inventory      map[string]Item `json:"inventory"`
	EventLog       []Event         `json:"event_log"`

	Mu sync.RWMutex
}

// inicializa inventario con datos por defecto
func NewInitialState() *ReplicatedState {
	return &ReplicatedState{
		SequenceNumber: 0,
		Inventory: map[string]Item{
			"LAPICES":  {Quantity: 100, Price: 120},
			"LIBROS":   {Quantity: 50, Price: 15500},
			"BORRADOR": {Quantity: 200, Price: 50},
			"CUADERNO": {Quantity: 75, Price: 2500},
		},
		EventLog: make([]Event, 0),
	}
}

// guarda estado en archivo json en disco
func (s *ReplicatedState) Persist(nodeID int) error {

	fileName := fmt.Sprintf(InventoryFile, nodeID)

	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		fmt.Printf("❌ [DEBUG] Error al convertir a JSON: %v\n", err)
		return fmt.Errorf("error al serializar el estado: %w", err)
	}

	err = os.WriteFile(fileName, data, 0644)
	if err != nil {
		fmt.Printf("❌ [DEBUG] Error FATAL al escribir en archivo %s: %v\n", fileName, err)
		return fmt.Errorf("error al escribir el archivo %s: %w", fileName, err)
	}

	fmt.Printf("✅ [DEBUG] ¡ESCRITURA EXITOSA en %s! Secuencia guardada: %d\n", fileName, s.SequenceNumber)
	return nil
}

// carga estado desde disco o crea uno nuevo
func (s *ReplicatedState) Load(nodeID int) error {
	fileName := fmt.Sprintf(InventoryFile, nodeID)
	data, err := os.ReadFile(fileName)
	if err != nil {
		if os.IsNotExist(err) {

			fmt.Printf("⚠️ No se encontró archivo de estado (%s). Inicializando estado por defecto...\n", fileName)
			*s = *NewInitialState()
			return s.Persist(nodeID)
		}
		return fmt.Errorf("error al leer el archivo %s: %w", fileName, err)
	}

	if err := json.Unmarshal(data, s); err != nil {
		return fmt.Errorf("error al deserializar el estado: %w", err)
	}
	fmt.Printf("✅ Estado recuperado de %s. Secuencia final: %d.\n", fileName, s.SequenceNumber)
	return nil
}

// aplica cambios sin bloqueo para evitar deadlock
func (s *ReplicatedState) ApplyEvent(event Event) {

	// valida secuencia para mantener consistencia
	if event.Seq > s.SequenceNumber+1 {
		fmt.Printf("❌ Error de secuencia: Evento %d recibido, se esperaba %d\n", event.Seq, s.SequenceNumber+1)
		return
	}

	if event.Seq <= s.SequenceNumber {
		
		return
	}

	s.SequenceNumber = event.Seq
	item, ok := s.Inventory[event.Item]
	if !ok {

		fmt.Printf("⚠️ Ítem %s no encontrado, creando con valores por defecto.\n", event.Item)
		item = Item{}
	}

    // actualiza inventario segun operacion
	switch event.Op {
	case OpSetQuantity:
		item.Quantity = event.Value
		fmt.Printf("  -> Evento %d: Modificada cantidad de %s a %d.\n", event.Seq, event.Item, event.Value)
	case OpSetPrice:
		item.Price = event.Value
		fmt.Printf("  -> Evento %d: Modificado precio de %s a %d.\n", event.Seq, event.Item, event.Value)
	default:
		fmt.Printf("❌ Operación desconocida: %s\n", event.Op)
		return
	}

	s.Inventory[event.Item] = item
	s.EventLog = append(s.EventLog, event)
}