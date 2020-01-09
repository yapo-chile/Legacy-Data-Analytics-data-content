# Base pipeline 

This micro services is base micro services. 

### Build
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


docker run -v /Users/luisbarrera/Documents/secrets/dw_db:/app/db-secret \
           -e APP_DB_SECRET=/app/db-secret \
           containers.mpi-internal.com/yapo/content-seller-return-over-current:feat_seller-return-over-current