# liquidity-content pipeline 

## Description

This process make querys in dwh tables ad and big_sellers_detail to get data about published o deleted ads in a day. In the information obtained is the aproval date, removal date and reason if appropriate, category of ad and his type, seller and his type.
The consulted data is left in a parquet named `ads.parquet` in the S3 Bucket `schibsted-spt-common-dev` in path `/dev/insights/liquidity/yapo/content/year={year}/month={month}/day={day}/` where `{year}`, `{month}` and `{day}` belong to the day for which the query was executed. 
This process supports two dates, date_from and date_to, but its execution is for one day since the path where the files are left is daily, so if the date_from is different from the date_to, the process will run to each day in the indicated period, leaving a file in the corresponding path per day.


## Pipeline Implementation Details

|   Field           | Description                                                            |
|-------------------|------------------------------------------------------------------------|
| Input Source      | Database dwh, table ods.ad and stg.big_sellers_detail                  |
| Output Source     | S3 Bucket schibsted-spt-common-dev                                     |
| Schedule          | One time at day, 03:00                                                 |
| Github Repository | github.mpi-internal.com/Yapo/bi-insights/blob/master/dw_blocketdb/liquidity/liquidity-content/run_liquidity_content.sh |
| Rundeck Access    | Project: data jobs ; Job: Liquidity - Ads contents to s3               |
| Associated Report | Unknow                                                                 |
|-------------------|------------------------------------------------------------------------|

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
           containers.mpi-internal.com/yapo/liquidity-content:[TAG]
```

### Run micro services with parameters

```
docker run -v /local-path/secrets/pulse:/app/pulse-secret \
           -v /local-path/secrets/db-secret:/app/db-secret \
           -e APP_PULSE_SECRET=/app/pulse-secret \
           -e APP_DB_SECRET=/app/db-secret \
           containers.mpi-internal.com/yapo/liquidity-content:[TAG] \
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
