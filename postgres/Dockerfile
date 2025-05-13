FROM postgres:latest

# Instalar gettext para usar envsubst
RUN apt-get update && apt-get install -y gettext

# Copiar archivos al contenedor
COPY init.sql.template /init.sql.template
COPY entrypoint.sh /usr/local/bin/custom-entrypoint.sh

# Dar permisos
RUN chmod +x /usr/local/bin/custom-entrypoint.sh

# Usar nuestro entrypoint personalizado
ENTRYPOINT ["/usr/local/bin/custom-entrypoint.sh"]
