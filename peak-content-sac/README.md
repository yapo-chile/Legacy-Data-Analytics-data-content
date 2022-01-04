# peak-content-sac pipeline 

# peak-content-sac

## Description

This pipeline makes available data associated with customer service performed by the SAC team. From this data you can obtain information associated with reasons for requesting attention, date of opening and resolution of the attention case, attention channel, results of satisfaction surveys, among other data more associated with the requesting client. This data is obtained through the interaction with various endpoints available on the Zendesk and Surveypal platforms and is recorded in our datawarehouse after being transformed into this pipeline.

Finally, it is important to mention that regarding the execution and re-execution strategy, it is not necessary in this case to pass variables "from" "to" (dates vars) because these are obtained by consulting the last data recorded in the output data tables at the DWH. That said, to re-execute a time range and obtain this data again, it is necessary to delete this range in the aforementioned output tables and this pipeline will automatically obtain the data for the time range in question, up to the current day.


## Pipeline Implementation Details

|   Field           | Description                                                                |
|-------------------|----------------------------------------------------------------------------|
| Input Source      | Zendesk API and Surveypal API endpoints                                    |
| Output Source     | DWH: dm_analysis.surveypal_csat_answers                                    |
|                          dm_analysis.test_zendesk_tickets                                      |
| Schedule          | hourly                                                                     |
| Rundeck Access    | data jobs: GLOBAL-METRIC: Peak - Content-SAC                               |
| Associated Report | Content&Sac SQUAD:                                                         |
|                       https://tableau.mpi-internal.com/#/site/sch-cl-yapo/workbooks/12268/views|
|                     SAC KPIs:                                                                  |
|                       https://tableau.mpi-internal.com/#/site/sch-cl-yapo/workbooks/7559/views |
|                                                                                                |


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
           registry.gitlab.com/yapo_team/legacy/data-analytics/data-content:latest_peak-content-sac
```

### Run micro services with parameters

```
docker run -v /local-path/secrets/pulse:/app/pulse-secret \
           -v /local-path/secrets/db-secret:/app/db-secret \
           -e APP_PULSE_SECRET=/app/pulse-secret \
           -e APP_DB_SECRET=/app/db-secret \
           registry.gitlab.com/yapo_team/legacy/data-analytics/data-content:latest_peak-content-sac
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
