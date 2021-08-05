# Elasticsearch Backend for the stac-fastapi server


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

