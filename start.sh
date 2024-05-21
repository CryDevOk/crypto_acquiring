docker build -f ./base_image_python/Dockerfile -t base_image:latest ./base_image_python
docker build -f ./evm_handler/Dockerfile -t proc_eth:latest ./evm_handler

# Create DataBases
docker-compose up -d postgres
export PGPASSWORD=postgres
wait_for_postgresql() {
    until psql -h localhost -p 5432 -U postgres -d postgres -c '\q' &>/dev/null; do
        echo "[INFO] PostgreSQL is not yet ready. Waiting..."
        sleep 1
    done
    echo "[INFO] PostgreSQL is ready for connections."
}

wait_for_postgresql

psql -h localhost -U postgres -c \
    "CREATE DATABASE proc_api WITH OWNER "postgres" ENCODING 'UTF8' LC_COLLATE = 'en_US.UTF-8' LC_CTYPE = 'en_US.UTF-8' TEMPLATE template0;"\
    2> /dev/null || echo "[INFO] Database proc_api already exists"

psql -h localhost -U postgres -c \
    "CREATE DATABASE eth_sepolia WITH OWNER "postgres" ENCODING 'UTF8' LC_COLLATE = 'en_US.UTF-8' LC_CTYPE = 'en_US.UTF-8' TEMPLATE template0;"\
    2> /dev/null || echo "[INFO] Database eth_sepolia already exists"

psql -h localhost -U postgres -c '\list'

# Deploy the services
docker-compose up -d eth_sepolia

