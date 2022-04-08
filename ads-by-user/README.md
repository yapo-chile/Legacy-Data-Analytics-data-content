# ads-by-user pipeline 

# ads-by-user

## Description

Ads by user Etl processes params from verticals as it would be cars, inmo and big sellers,
then store them in datawarehouse as appended method so a historical daya would be always
available.

## Pipeline Implementation Details

|   Field           | Description                                                                |
|-------------------|----------------------------------------------------------------------------|
| Input Source      | DWH ods.ads                                                                |
| Output Source     | Schema dwh ods.ads_by_user and statistics public.ads_by_user               |
| Schedule          | 02:30 everyday                                                             |
| Rundeck Access    | Specify rundeck environment (test/data jobs) and rundeck ETL name          |
| Associated Report | Specify name and URL of tableau report (if applies)                        |


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
           containers.mpi-internal.com/yapo/ads-by-user:[TAG]
```

### Run micro services with parameters

```
docker run -v /local-path/secrets/pulse:/app/pulse-secret \
           -v /local-path/secrets/db-secret:/app/db-secret \
           -e APP_PULSE_SECRET=/app/pulse-secret \
           -e APP_DB_SECRET=/app/db-secret \
           containers.mpi-internal.com/yapo/ads-by-user:[TAG] \
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
