#!/bin/bash
set -e

# Expandir variables del template en un archivo final
envsubst < /init.sql.template > /docker-entrypoint-initdb.d/init.sql

# Ejecutar la entrada predeterminada de PostgreSQL
exec docker-entrypoint.sh "$@"
