```` bash
docker compose -f docker-compose.kafka.yml exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --create --topic payments --partitions 6 --replication-factor 3
````