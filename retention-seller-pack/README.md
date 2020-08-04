# retention-seller-pack pipeline 

## Description

This pipeline have 3 steps

- RetentionSellerPack:  This step extract monthly data from ods.packs about the the active packs in the month, pack puchased in month, number of sellers who bought packs this month, the number of sellers that buy again after a month, and sellers that hace pack last month, but this month doesnt buy. Also, get the accrual (Devengo) of previus metrics.

- RetentionSellerPackDetail: This step extract the same metrics of RetentionSellerPack but grouped by numbers of days and numbers of slots of active packs.

- SendEmailSellersPackLeak: This step extract fata about sellers packs leak (fuga), that is, sellers who had a pack last month, but this month they did not buy one.



## Pipeline Implementation Details

|   Field           | Description                                                                |
|-------------------|----------------------------------------------------------------------------|
| Input Source      | DWH ods.packs                                                              |
| Output Source     | stg.retention_sellers_packs, stg.retention_sellers_packs_detail            |
| Schedule          | One time at month (actually in project test of rundeck is manual)          |
| Rundeck Access    | Data_jobs : Content - Retention sellers packs                              |
| Associated Report | --                                                                         |


### Build
```
make docker-build
```

### Run micro services
```
docker run -v /local-path/secrets/pulse:/app/pulse-secret \
           -v /local-path/secrets/db-secret:/app/db-secret \
           -e APP_PULSE_SECRET=/app/pulse-secret \
           -e APP_DB_SECRET=/app/db-secret \
           containers.mpi-internal.com/yapo/retention-seller-pack:[TAG]
```

### Run micro services with parameters

```
docker run -v /local-path/secrets/pulse:/app/pulse-secret \
           -v /local-path/secrets/db-secret:/app/db-secret \
           -e APP_PULSE_SECRET=/app/pulse-secret \
           -e APP_DB_SECRET=/app/db-secret \
           containers.mpi-internal.com/yapo/retention-seller-pack:[TAG] \
           -date_from=YYYY-MM-DD \
           -date_to=YYYY-MM-DD
```

### Adding Rundeck token to Travis

If we need to import a job into Rundeck, we can use the Rundeck API
sending an HTTTP POST request with the access token of an account.
To add said token to Travis (allowing travis to send the request),
first, we enter the user profile:
```
<rundeck domain>:4440/user/profile
```
And copy or create a new user token.

Then enter the project settings page in Travis
```
htttp://<travis server>/<registry>/<project>/settings
```
And add the environment variable RUNDECK_TOKEN, with value equal
to the copied token
