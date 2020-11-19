# core-ad-sellers pipeline 

# core-ad-sellers

## Description

Daily this pipeline makes available into DWH the data related to sellers of ours ads whatever its classification private or professional as well as a detail available related to sellers pro in order to know in which categories a seller is professional.

## Pipeline Implementation Details

|   Field           | Description                                                                |
|-------------------|----------------------------------------------------------------------------|
| Input Source      | Blocket:    public/blocket_{year}.action_states                            |
|                   |             public/blocket_{year}.ad_actions                               |
|                   |             public/blocket_{year}.ads                                      |
|                   |             public/blocket_{year}.action_params                            |
|                   |             public/blocket_{year}.users                                    |
|                   |             public/blocket_{year}.accounts                                 |
|                   |             public/blocket_{year}.ad_params                                |
| Output Source     | DWH:    stg.account                                                        |
|                   |         stg.seller_created_daily                                           | 
|                   |         stg.seller_pro                                                     |   
|                   |         ods.seller                                                         |   
|                   |         ods.seller_pro_details                                             |
| Schedule          | 00:10 Daily                                                                |
| Rundeck Access    | data jobs: CORE: Ad-sellers                                                |
| Associated Report | TBD                                                                        |


### Build
```
make docker-build
```

### Run micro services
```
sudo docker pull containers.mpi-internal.com/yapo/core-ad-sellers:latest
sudo docker run --rm --net=host \
                        -v /home/bnbiuser/secrets/dw_db:/app/db-secret \
                        -v /home/bnbiuser/secrets/blocket_db:/app/blocket-secret \
                        -v /home/bnbiuser/secrets/smtp:/app/smtp-secret \
                        -e APP_DB_SECRET=/app/blocket-secret \
                        -e APP_DW_SECRET=/app/db-secret \
                        -e APP_SMTP_SECRET=/app/smtp-secret \
                        containers.mpi-internal.com/yapo/core-ad-sellers:latest \
                        -email_from="noreply@yapo.cl" \
                        -email_to="data_team@adevinta.com"
```

### Run micro services with parameters

```
sudo docker pull containers.mpi-internal.com/yapo/core-ad-sellers:latest
sudo docker run --rm --net=host \
                        -v /home/bnbiuser/secrets/dw_db:/app/db-secret \
                        -v /home/bnbiuser/secrets/blocket_db:/app/blocket-secret \
                        -v /home/bnbiuser/secrets/smtp:/app/smtp-secret \
                        -e APP_DB_SECRET=/app/blocket-secret \
                        -e APP_DW_SECRET=/app/db-secret \
                        -e APP_SMTP_SECRET=/app/smtp-secret \
                        containers.mpi-internal.com/yapo/core-ad-sellers:latest \
                        -email_from="noreply@yapo.cl" \
                        -email_to="data_team@adevinta.com"
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
