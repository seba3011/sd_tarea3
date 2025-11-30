# Sistema de Inventario Distribuido

## Integrante

* **Nombre:** Sebastián López
* **Rol:** 202173020-6

---

## Configuración de Red

El sistema requiere que los siguientes puertos estén libres en las máquinas correspondientes para la comunicación RPC:

| Nodo ID | Dirección IP | Puerto |
| :--- | :--- | :--- |
| **1** | `10.10.31.76` | **8081** |
| **2** | `10.10.31.77` | **8082** |
| **3** | `10.10.31.78` | **8083** |

---

## Instrucciones de Ejecución

El sistema utiliza scripts de shell ubicados en la carpeta `scripts/` para facilitar el inicio y la parada de los nodos.

### 1. Iniciar los Nodos (Servidores)

Debe conectarse a cada máquina virtual y ejecutar el script desde la carpeta `sd_tarea3/scripts/`:

## Orden de ejecucion

**En vm3 desde scripts:**
```bash
./run_node.sh 3
```
**En vm2 desde scripts:**
```bash
./run_node.sh 2
```
**En vm1 desde scripts:**
```bash
./run_node.sh 1
```
**En vm1 desde sd_tarea3:**
```bash
go run client/client.go
```
realizar consultas requeridas 

## Ejemplo matar nodo X
cerrar codigo con ctl+c (desde /scripts)
```bash
./fail_node.sh x
```

