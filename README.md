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

## Running the development server

**Option 1 (virtual environment):**

This option makes use of traditional virtual environment. You might need to specify the path
to the uvicorn binary e.g. `/venv/bin/uvicorn` to make sure you are using the right environment.

    ```bash
    export STAC_ELASTICSEARCH_SETTINGS=stac_fastapi.elasticsearch.settings
    ```
    
    ```bash
    uvicorn stac_fastapi.elasticsearch.app:app --reload
    ```
**Option 2 (Docker):**

This option makes use of Docker compose and includes some sample data. It will start a local elasticsearch instance, 
populate with some data and start the application which will reload if you modify the code or configuration.
   ```bash
   make run-sample-elasticsearch
   ```

You can change the default settings by creating a file in `/conf/settings.d` and adding `include_dir(base_dir / 'settings.d')` 
to `/conf/settings.py`
Here you can set:
   - `ELASTICSEARCH_CONNECTION`
   - `COLLECTION_INDEX`
   - `ITEM_INDEX`
   - `ASSET_INDEX`

You could use this to point at production or staging data instead of the local instance.

### Demo Application

You can use docker-compose to create a demo instance. This will create an elasticsearch node, add some sample data and run the API.

```bash
make run-sample-elasticsearch
```

**NOTE: You will need to build the image first** `docker-compose build`
