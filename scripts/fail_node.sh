#!/bin/bash
# fail_node.sh <node_id>

NODE_ID=$1
PID_FILE="node_$NODE_ID.pid"

if [ -z "$NODE_ID" ]; then
    echo "Uso: ./fail_node.sh <node_id>"
    exit 1
fi

if [ ! -f "$PID_FILE" ]; then
    echo "❌ Error: Archivo PID para nodo $NODE_ID no encontrado. ¿Está corriendo?"
    exit 1
fi

PID=$(cat $PID_FILE)

echo "Simulando fallo (fail-stop) para Server Node $NODE_ID (PID $PID)..."
# Enviar señal de terminación (SIGTERM) para que el nodo pueda guardar su estado
kill -TERM $PID

# Esperar un momento para que el proceso termine
sleep 1

if ps -p $PID > /dev/null; then
    echo "⚠️ El proceso no terminó. Terminación forzada (kill -9)."
    kill -9 $PID
fi

rm $PID_FILE
echo "✅ Server Node $NODE_ID detenido (fallado)."
echo "El sistema debe detectar la falla y elegir un nuevo líder."