import environ


INI_GOOGLE = environ.secrets.INISecrets.from_path_in_env("APP_GOOGLE_SECRET")
INI_DB = environ.secrets.INISecrets.from_path_in_env("APP_DB_SECRET")


@environ.config(prefix="APP")
class AppConfig:
    """
    AppConfig Class representing the configuration of the application
    """

    @environ.config(prefix="GOOGLE")
    class GoogleConfig:
        """
        GoogleConfig class represeting the configuration to access
        google sheets
        """
        type_google: str = INI_GOOGLE.secret(
            name="type", default=environ.var())
        project_id: str = INI_GOOGLE.secret(
            name="projectid", default=environ.var())
        private_key_id: str = INI_GOOGLE.secret(
            name="privatekeyid", default=environ.var())
        private_key: str = INI_GOOGLE.secret(
            name="privatekey", default=environ.var())
        client_email: str = INI_GOOGLE.secret(
            name="clientemail", default=environ.var())
        client_id: str = INI_GOOGLE.secret(
            name="clientid", default=environ.var())
        auth_uri: str = INI_GOOGLE.secret(
            name="authuri", default=environ.var())
        token_uri: str = INI_GOOGLE.secret(
            name="tokenuri", default=environ.var())
        auth_provider_x509_certurl: str = INI_GOOGLE.secret(
            name="authproviderx509certurl", default=environ.var())
        client_x509_cert_url: str = INI_GOOGLE.secret(
            name="clientx509certurl", default=environ.var())
        scope: str = 'https://www.googleapis.com/auth/drive.readonly'

    @environ.config(prefix="DB")
    class DBConfig:
        """
        DBConfig Class representing the configuration to access the database
        """
        host: str = INI_DB.secret(name="host", default=environ.var())
        port: int = INI_DB.secret(name="port", default=environ.var())
        name: str = INI_DB.secret(name="dbname", default=environ.var())
        user: str = INI_DB.secret(name="user", default=environ.var())
        password: str = INI_DB.secret(name="password", default=environ.var())
        table_direct: str = "dm_peak.advertising_direct_sales_revenues"
        table_network: str = "dm_peak.advertising_network_sales_revenues"
    google = environ.group(GoogleConfig)
    db = environ.group(DBConfig)


def getConf():
    return environ.to_config(AppConfig)
