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

# Post dataset segment

```
curl --location --request POST 'localhost:6077/datasets/MY_DATASET/segments/S-0001-xA2bc98' \
--form 'segment_data=@"/data/S-0001-data.parquet"' \
--form 'segment_keys="{\"data_id\":\"S-0001\", \"hash_id\":\"xA2bc98\"}";type=application/json'
```

This will add the tabular data in S-0001-data.parquet as a segment within the dataset `MY_DATASET`, such that it can be retrived by running:

```
SELECT * FROM MY_DATASET WHERE SEGMENT_ID = 'S-0001-xA2bc98`
```
