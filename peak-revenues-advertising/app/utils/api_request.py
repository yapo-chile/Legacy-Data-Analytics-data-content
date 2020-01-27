import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from infraestructure.conf import getConf


def get_dict_google_credentials(google_conf,
                                json_filename: str):
    """
    Function that get google sheet credentials.
    Returns a json file with credentials.
    """
    dict_credentials = {
        "type": google_conf.type_google,
        "project_id": google_conf.project_id,
        "private_key_id": google_conf.private_key_id,
        "private_key": google_conf.private_key,
        "client_email": google_conf.client_email,
        "client_id": google_conf.client_id,
        "auth_uri": google_conf.auth_uri,
        "token_uri": google_conf.token_uri,
        "auth_provider_x509_cert_url": google_conf.auth_provider_x509_certurl,
        "client_x509_cert_url": google_conf.client_x509_cert_url
    }
    with open(json_filename, "w") as file_io:
        file_io.write(str(dict_credentials). \
                            replace('\\\\', '\\'). \
                            replace('\'', '"'))

def get_drive_sheet_dataframe(sheet_name: str,
                              conf: getConf) -> pd.DataFrame:
    """
    Get data an pandas dataframe from google drive sheet.
    """
    scope = [conf.google.scope]
    json_filename = 'credentials.json'
    get_dict_google_credentials(conf.google,
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
