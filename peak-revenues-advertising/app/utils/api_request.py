
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from infraestructure.conf import getConf


def get_dict_credentials(conf: getConf,
                         json_filename: str):
    dict_credentials = {
        "type": conf.google.type_google,
        "project_id": conf.google.project_id,
        "private_key_id": conf.google.private_key_id,
        "private_key": conf.google.private_key,
        "client_email": conf.google.client_email,
        "client_id": conf.google.client_id,
        "auth_uri": conf.google.auth_uri,
        "token_uri": conf.google.token_uri,
        "auth_provider_x509_cert_url": conf.google.auth_provider_x509_certurl,
        "client_x509_cert_url": conf.google.client_x509_cert_url
    }
    with open(json_filename, "w") as file_io:
        file_io.write(str(dict_credentials). \
                            replace('\\\\', '\\'). \
                            replace('\'', '"'))

def get_drive_sheet_dataframe(sheet_name: str,
                              conf: getConf) -> pd.DataFrame:
    scope = [conf.google.scope]
    json_filename = 'credentials.json'
    get_dict_credentials(conf,
                         json_filename)
    credentials = ServiceAccountCredentials.\
                    from_json_keyfile_name(json_filename,
                                           scope)
    client = gspread.authorize(credentials)
    sheet = client.open("okrs_advertising")
    sheet_names = sheet.worksheet(sheet_name)
    df_data = pd.DataFrame(sheet_names.get_all_values())
    df_data.columns = df_data.iloc[0]
    df_data.drop(df_data.index[0], inplace=True)
    return df_data
