# send_email_besedo pipeline 
# send_email_besedo
This pipeline get quantities of time that take manual ads review process for each queue and send an email with the amounts according to delay and the corresponding percentage. 
## Description
This pipeline get data of review_log, ad_actions and actions_state, and calculate the time in minutes it takes to review the ads of the day. When they are calculated, it classifies them according to time periods and get the numbers of ads for each time lapse according his queue.
Finally save data in excel format and send this information for email.

## Pipeline Implementation Details

|   Field           | Description                                                                |
|-------------------|----------------------------------------------------------------------------|
| Input Source      | Blocket, schema: public: review_logs, ad_actions, admins, action_states    |
|                   |          schema: blocket${current_year}: ad_actions, action_states         |
|                   |          schema: blocket${last_year}: ad_actions, action_states            |
| Output Source     | Email                                                                      |
| Schedule          | 04:00                                                                      |
| Rundeck Access    | Send email besedo                                                          |
| Associated Report | None                                                                       |


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
           containers.mpi-internal.com/yapo/send_email_besedo:[TAG]
```

### Run micro services with parameters

```
docker run -v /local-path/secrets/pulse:/app/pulse-secret \
           -v /local-path/secrets/db-secret:/app/db-secret \
           -e APP_PULSE_SECRET=/app/pulse-secret \
           -e APP_DB_SECRET=/app/db-secret \
           containers.mpi-internal.com/yapo/send_email_besedo:[TAG] \
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
