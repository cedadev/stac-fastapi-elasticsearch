# Elasticsearch Backend for the stac-fastapi server

[![stac-fastapi-elasticsearch](https://github.com/cedadev/stac-fastapi-elasticsearch/actions/workflows/test.yml/badge.svg)](https://github.com/cedadev/stac-fastapi-elasticsearch/actions/workflows/test.yml)

## Getting started

Copy settings.py.tmpl and remove the tmpl extension. 
Fill in 
- ELASTICSEARCH_HOST
- api_key

Create a virtualenv and install requirements
```bash
python -m venv venv
./venv/bin/activate
pip intall -r requirements.txt
```

## Running the server

```bash
export STAC_ELASTICSEARCH_SETTINGS=stac_fastapi.elasticsearch.settings
```

```bash
uvicorn stac_fastapi.elasticsearch.app:app --reload
```

### Demo Application

You can use docker-compose to create a demo instance. This will create an elasticsearch node, add some sample data and run the API.

```bash
make run-sample-elasticsearch
```

**NOTE: You will need to build the image first** `docker-compose build`
