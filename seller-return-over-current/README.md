# Seller return over current

This micro services create metric **seller return over current**. 

## Source data
Data is consume from **data warehouse**.

## Destiny data
Data is store in **dm_peak.seller_return_over_current** table from our **data warehouse**.


## Build
```
make docker-build
```

### Run micro services
```
docker run -v /local-path/secrets/db-secret:/app/db-secret \
           -e APP_DB_SECRET=/app/db-secret \
           containers.mpi-internal.com/yapo/content-seller-return-over-current:[TAG]
```

### Run micro services with parameters

```
docker run -v /local-path/secrets/db-secret:/app/db-secret \
           -e APP_DB_SECRET=/app/db-secret \
           containers.mpi-internal.com/yapo/content-seller-return-over-current:[TAG] \
           -date_from=YYYY-MM-DD \
           -date_to=YYYY-MM-DD
```