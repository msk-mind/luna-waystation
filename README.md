# luna-waystation

[![Docker Image CI](https://github.com/msk-mind/luna-waystation/actions/workflows/docker-image.yml/badge.svg)](https://github.com/msk-mind/luna-waystation/actions/workflows/docker-image.yml)

Routes segments of datasets (rows and columns of tabular data) into corresponding parquet tables on S3 storage.

Available on docker: https://hub.docker.com/repository/docker/mskmind/luna-waystation

```
docker run -d -p 6077:6077 \
  -e S3_ACCESS_KEY=username \
  -e S3_SECRET_KEY=password \
  -e S3_ROOT_URL=http://localhost:9000/datasets 
  mskminddev/luna-waystation:latest
```
