#!/bin/bash
set -e

# Espera a que el controlador de Pinot esté disponible
until curl -s http://pinot-controller:9000/health > /dev/null; do
  echo "Esperando a que Pinot Controller esté disponible..."
  sleep 5
done

echo "Pinot Controller está disponible. Creando tablas..."

# Crear esquema
bin/pinot-admin.sh AddSchema \
  -schemaFile /schemas/hechos_accidentes_schema.json \
  -controllerHost pinot-controller \
  -controllerPort 9000 \
  -exec

# Crear tabla
bin/pinot-admin.sh AddTable \
  -schemaFile /schemas/hechos_accidentes_schema.json \
  -tableConfigFile /tables/hechos_accidentes_realtime_table.json \
  -controllerHost pinot-controller \
  -controllerPort 9000 \
  -exec

echo "Esquema y tabla creados correctamente."
