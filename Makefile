#!make
APP_HOST ?= 0.0.0.0
APP_PORT ?= 8080
EXTERNAL_APP_PORT ?= ${APP_PORT}
run_docker = docker-compose run --rm \
                -p ${EXTERNAL_APP_PORT}:${APP_PORT} \
                -e APP_HOST=${APP_HOST} \
                -e APP_PORT=${APP_PORT} \
                app-elasticsearch

.PHONY: image
image:
	docker-compose build

.PHONY: docker-run
docker-run: image
	$(run_docker)

.PHONY: docker-shell
docker-shell:
	$(run_docker) /bin/bash

.PHONY: test-elasticsearch 
test-elasticsearch: run-sample-elasticsearch
	$(run_docker) /bin/bash -c './scripts/wait-for-it.sh database:9200 && cd /app/tests && pytest -s -k test_search_for_collection' 

.PHONY: run-database
run-database:
	docker-compose run --rm database

.PHONY: run-sample-elasticsearch
run-sample-elasticsearch:
	docker-compose run --rm loadsample-elasticsearch

.PHONY: test
test: test-elasticsearch

.PHONY: elasticsearch-install
elasticsearch-install:
	pip install -r requirements.txt && \
    pip install -e .

