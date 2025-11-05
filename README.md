# mlops-datapipeline
```
docker compose run --rm airflow-webserver airflow db init
docker compose run --rm airflow-webserver airflow users create \
  --username admin --firstname Admin --lastname User \
  --role Admin --email admin@example.com
```
docker compose up -d

```
If there is still an error then run 
```
docker compose down
docker compose run --rm airflow-webserver airflow db init
docker compose up -d
```
```
docker compose run --rm airflow-webserver airflow users create \
  --username admin \
  --firstname Admin \
  --lastname yannick \
  --role Admin \
  --email admin@example.com
```