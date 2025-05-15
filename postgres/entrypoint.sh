#!/bin/bash
set -e

# Ruta CORRECTA para los scripts de inicialización
INIT_DIR="/docker-entrypoint-initdb.d"

# Crear directorio si no existe
mkdir -p "$INIT_DIR"

# Procesar template con TODAS las variables de entorno
echo "Procesando init.sql.template..."
envsubst < "/usr/local/bin/init.sql.template" > "$INIT_DIR/init.sql"

# Debug: Mostrar archivo generado
echo "=== Contenido de init.sql ==="
cat "$INIT_DIR/init.sql"
echo "============================"

# Ejecutar entrypoint original
exec /usr/local/bin/docker-entrypoint.sh "$@"