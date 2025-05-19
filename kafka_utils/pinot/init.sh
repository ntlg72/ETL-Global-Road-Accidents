#!/bin/bash

# Espera a que el controlador esté activo
echo "Esperando a que el Pinot Controller esté disponible..."
sleep 10

# Agrega el schema
echo "Agregando schema..."
bin/pinot-admin.sh AddSchema \
  -schemaFile /schemas/road_accidents_schema.json \
  -controllerHost pinot-controller \
  -controllerPort 9000 \
  -exec

# Agrega la tabla
echo "Agregando tabla..."
bin/pinot-admin.sh AddTable \
  -tableConfigFile /tables/road_accidents_realtime_table.json \
  -schemaFile /schemas/road_accidents_schema.json \
  -controllerHost pinot-controller \
  -controllerPort 9000 \
  -exec
