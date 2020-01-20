# Seller return over current and past

This micro services create metric **seller return over current** and **seller return over past**. 

## Source data
Data is consume from **data warehouse**.

## Destiny data
The Data is store in two tables. This  **dm_peak.seller_return_over_current** table from our **data warehouse**.
Also 


## Build
```
make docker-build
```

### Run micro services
```
docker run -v /local-path/secrets/db-secret:/app/db-secret \
           -e APP_DB_SECRET=/app/db-secret \
           containers.mpi-internal.com/yapo/content-seller-return-over-current-past:[TAG]
```

### Run micro services with parameters

```
docker run -v /local-path/secrets/db-secret:/app/db-secret \
           -e APP_DB_SECRET=/app/db-secret \
           containers.mpi-internal.com/yapo/content-seller-return-over-current-past:[TAG] \
           -date_from=YYYY-MM-DD \
           -date_to=YYYY-MM-DD
```