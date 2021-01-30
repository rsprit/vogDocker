# Running in docker

## Installation

```bash
sudo apt-get install docker.io docker-compose
```

## Services

Currently two services are defined in `docker-compose.yaml`:

* db

  This is the Mysql database server.

  You start the db service with
  ```bash
  docker-compose up -d db
  ```
  Because `app` has a dependency on `db`
  it will also be started when you start the `app` container.

  Currently port 3306 is exposed from the db service to facilitate local development. This is **not** required/recommended in production.

* app

  This is the application (i.e. the FastAPI server). By default, it is hosted in `uvicorn`, but
  this can be changed in `docker-compose.yaml` to `hypercorn`.

  You start the app service with
  ```bash
  docker-compose up -d app
  ```

  This container also contains to data loading jobs `load-vog` and `load-taxa`. Because these are not long-running services, they have to be started by
  ```bash
  docker-compose run --rm app <job>
  ```
  job is either `load-vog` or `load-taxa`.

Data is stored on persistent volumes, therefore
```bash
docker-compose down
```
will leave the database and other data intact, whereas 
```bash
docker-compose down -v
```
will remove the volumes and you will have to start from scratch.

You can inspect the logs with
```bash
docker-compose logs [service...]
```
A running log is obtained by
```bash
docker-compose logs -f
```

All the commands above assume that you are running docker-compose from the directory where the docker-compose.yaml is stored. Otherwise (e.g. in crontab) you will have to specify the full path 
when you invoke docker-compose.
```bash
docker-compose -f /path/to/docker-compose.yaml run --rm app load-vog
```

## Building the images

If you change the definition of the app image (i.e. in the `Dockerfile`) or you want to include new/changed sources in the container, you will have to rebuild them by issueing
```bash
docker-compose build
```
