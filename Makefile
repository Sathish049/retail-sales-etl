.PHONY: data up down logs test psql reset

data:        ## generate 14 days of fake sales files
	python scripts/generate_data.py --start 2026-10-01 --days 14

up:          ## build image and start Airflow (UI on http://localhost:8080)
	docker compose up -d --build

down:        ## stop containers (keeps data)
	docker compose down

reset:       ## stop and wipe both databases
	docker compose down -v

logs:
	docker compose logs -f airflow-scheduler

test:        ## unit tests, no Docker needed
	pytest -q

psql:        ## open a SQL shell on the warehouse
	docker compose exec retail-db psql -U retail -d retail_dw
