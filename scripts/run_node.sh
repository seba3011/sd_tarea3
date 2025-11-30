#!/bin/bash
# run_node.sh <node_id> [primary_on_start]

NODE_ID=$1
START_AS_PRIMARY=$2

if [ -z "$NODE_ID" ]; then
    echo "Uso: ./run_node.sh <node_id> [primary_on_start]"
    exit 1
fi

echo "Iniciando Server Node con ID $NODE_ID..."

# Compilar el programa principal
go build -o server_node ../main.go

# Ejecutar el programa en segundo plano y guardar el PID
if [ "$START_AS_PRIMARY" = "primary_on_start" ]; then
    ./server_node $NODE_ID primary_on_start &
else
    ./server_node $NODE_ID &
fi

PID=$!
echo "Server Node $NODE_ID iniciado con PID $PID."
# Guardar el PID para el script de kill
echo $PID > node_$NODE_ID.pid