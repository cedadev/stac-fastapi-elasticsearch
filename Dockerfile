FROM python:3.8-slim as base

FROM base as builder
# Any python libraries that require system libraries to be installed will likely
# need the following packages in order to build
RUN apt-get update && apt-get install -y build-essential git

WORKDIR /app

# Copy the repo context
COPY . /app

RUN pip install -r ./requirements.txt && \
    pip install django-flexi-settings==0.1.1 && \
    pip install -e .[server]
