# core-ads-created-daily pipeline 

# core-ads-created-daily

## Description

Ads created daily pipeline that aims to obtain from blocket db the ads created on the day of the process, in addition to calculating the dates of approval and elimination / rejection of ads on the same process date.

## Pipeline Implementation Details

|   Field           | Description                                                                |
|-------------------|----------------------------------------------------------------------------|
| Input Source      | Blocket:   {public,                                                        |
|                   |             blocket_$current_year,                                         |
|                   |             blocket_$last_year}.action_states                              |
|                   |            {public,                                                        |
|                   |             blocket_$current_year,                                         |
|                   |             blocket_$last_year}.ad_actions                                 |
|                   |            {public,                                                        |
|                   |             blocket_$current_year,                                         |
|                   |             blocket_$last_year}.ads                                        |
|                   |            {public,                                                        |
|                   |             blocket_$current_year,                                         |
|                   |             blocket_$last_year}.action_params                              |
|                   |            {public,                                                        |
|                   |             blocket_$current_year,                                         |
|                   |             blocket_$last_year}.ad_params                                  |
|                   |            public.users                                                    |
|                   |            public.accounts                                                 |
| Output Source     | DWH:  stg.ad                                                               |
|                   |       ods.ad                                                               |
|                   |       ods.seller                                                           |
| Schedule          | logical precedence                                                         |
| Rundeck Access    | data jobs: CORE: Ads-created-daily                                         |
| Associated Report | TBD                                                                        |


### Build
```
make docker-build
```

### Run micro services
```
sudo docker pull containers.mpi-internal.com/yapo/core-ads-created-daily:latest
sudo docker run --rm -v /home/bnbiuser/secrets/dw_db:/app/db-secret \
                        -v /home/bnbiuser/secrets/blocket_db:/app/blocket-secret \
                        -e APP_DB_SECRET=/app/blocket-secret \
                        -e APP_DW_SECRET=/app/db-secret \
                        containers.mpi-internal.com/yapo/core-ads-created-daily:latest
```

### Run micro services with parameters

```
sudo docker pull containers.mpi-internal.com/yapo/core-ads-created-daily:latest
sudo docker run --rm -v /home/bnbiuser/secrets/dw_db:/app/db-secret \
                        -v /home/bnbiuser/secrets/blocket_db:/app/blocket-secret \
                        -e APP_DB_SECRET=/app/blocket-secret \
                        -e APP_DW_SECRET=/app/db-secret \
                        containers.mpi-internal.com/yapo/core-ads-created-daily:latest \
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
