package common

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// Constantes de Operación
const (
	OpSetQuantity = "SET_QTY"
	OpSetPrice    = "SET_PRICE"
	InventoryFile = "nodo_%d.json" // Formato: nodo_<ID>.json
)

// Item representa un artículo en el inventario.
type Item struct {
	Quantity int `json:"quantity"`
	Price    int `json:"price"`
}

// Event representa una modificación al inventario.
type Event struct {
	Seq   int    `json:"seq"`
	Op    string `json:"op"`
	Item  string `json:"item"`
	Value int    `json:"value"`
}

// ReplicatedState contiene el estado completo del nodo.
// Esto es lo que se persiste en nodo.json.
type ReplicatedState struct {
	SequenceNumber int             `json:"sequence_number"`
	Inventory      map[string]Item `json:"inventory"`
	EventLog       []Event         `json:"event_log"`
	// Mutex para proteger el acceso concurrente al estado.
	mu sync.RWMutex
}

// NewInitialState crea un estado inicial con 4 artículos predefinidos.
func NewInitialState() *ReplicatedState {
	return &ReplicatedState{
		SequenceNumber: 0,
		Inventory: map[string]Item{
			"LAPICES": {Quantity: 100, Price: 120},
			"LIBROS":  {Quantity: 50, Price: 15500},
			"BORRADOR": {Quantity: 200, Price: 50},
			"CUADERNO": {Quantity: 75, Price: 2500},
		},
		EventLog: make([]Event, 0),
	}
}

// Persist guarda el estado actual en el archivo nodo_<ID>.json.
func (s *ReplicatedState) Persist(nodeID int) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	fileName := fmt.Sprintf(InventoryFile, nodeID)
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("error al serializar el estado: %w", err)
	}

	err = os.WriteFile(fileName, data, 0644)
	if err != nil {
		return fmt.Errorf("error al escribir en el archivo %s: %w", fileName, err)
	}
	return nil
}

// Load carga el estado desde el archivo nodo_<ID>.json. Si el archivo no existe,
// devuelve el estado inicial.
func (s *ReplicatedState) Load(nodeID int) error {
	fileName := fmt.Sprintf(InventoryFile, nodeID)
	data, err := os.ReadFile(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			// Si no existe, inicializa con el estado por defecto y persiste.
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

// ApplyEvent aplica un evento de modificación al estado del inventario.
func (s *ReplicatedState) ApplyEvent(event Event) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if event.Seq > s.SequenceNumber+1 {
		fmt.Printf("❌ Error de secuencia: Evento %d recibido, se esperaba %d\n", event.Seq, s.SequenceNumber+1)
		return
	}

	if event.Seq <= s.SequenceNumber {
		// Evento ya aplicado. Se ignora.
		return
	}

	s.SequenceNumber = event.Seq
	item, ok := s.Inventory[event.Item]
	if !ok {
		// Asume que las modificaciones son a ítems existentes.
		fmt.Printf("⚠️ Ítem %s no encontrado, creando con valores por defecto.\n", event.Item)
		item = Item{}
	}

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