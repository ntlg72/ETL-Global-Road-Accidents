FROM postgres:latest

# Instalar gettext para usar envsubst
RUN apt-get update && apt-get install -y gettext && rm -rf /var/lib/apt/lists/*

# Crear directorio para scripts de inicialización
RUN mkdir -p /docker-entrypoint-initdb.d

# Copiar archivos al contenedor
COPY init.sql.template /usr/local/bin/
COPY entrypoint.sh /usr/local/bin/custom-entrypoint.sh

# Dar permisos de ejecución
RUN chmod +x /usr/local/bin/custom-entrypoint.sh

# Usar nuestro entrypoint personalizado
ENTRYPOINT ["/usr/local/bin/custom-entrypoint.sh"]

# Ejecutar el comando predeterminado de PostgreSQL
CMD ["postgres"]