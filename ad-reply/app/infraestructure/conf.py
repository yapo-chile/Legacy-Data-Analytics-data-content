import environ

INI_DB = environ.secrets.INISecrets.from_path_in_env("APP_DB_SECRET")
INI_BLOCKET = environ.secrets.INISecrets.from_path_in_env("APP_BLOCKET_SECRET")


@environ.config(prefix="APP")
class AppConfig:
    """
    AppConfig Class representing the configuration of the application
    """

    @environ.config(prefix="DB")
    class DBConfig:
        """
        DBConfig Class representing the configuration to access the database
        """
        #host: str = INI_DB.secret(name="host", default=environ.var())
        #port: int = INI_DB.secret(name="port", default=environ.var())
        #name: str = INI_DB.secret(name="dbname", default=environ.var())
        #user: str = INI_DB.secret(name="user", default=environ.var())
        #password: str = INI_DB.secret(name="password", default=environ.var())
        host = "54.144.226.106"
        port = 5432
        name = "dw_blocketdb_ch"
        user = "bnbiuser"
        password = "VE1bi@BN112AzLkOP"
    
    @environ.config(prefix="BLOCKET")
    class BlocketConfig:
        """
        DBConfig Class representing the configuration to access the database
        """
        #host: str = INI_BLOCKET.secret(name="host", default=environ.var())
        #port: int = INI_BLOCKET.secret(name="port", default=environ.var())
        #name: str = INI_BLOCKET.secret(name="dbname", default=environ.var())
        #user: str = INI_BLOCKET.secret(name="user", default=environ.var())
        #password: str = INI_BLOCKET.secret(name="password", default=environ.var())
        host = "200.29.173.148"
        port = 5432
        name = "blocketdb"
        user = "bnbiuser"
        password = "VE1bi@BN112AzLkOP"

    db = environ.group(DBConfig)
    blocket = environ.group(BlocketConfig)

def getConf():
    return environ.to_config(AppConfig)
