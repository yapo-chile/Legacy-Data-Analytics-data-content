import environ
import os
os.environ["APP_DB_SECRET"] = "/Users/ricardoalvarez/Projects/secrets/db-secret-prod"
os.environ["APP_DB_DEV_SECRET"] = "/Users/ricardoalvarez/Projects/secrets/db-secret-dev"

INI_DB = environ.secrets.INISecrets.from_path_in_env("APP_DB_SECRET")
DB_DEV = environ.secrets.INISecrets.from_path_in_env("APP_DB_DEV_SECRET")

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
        host: str = INI_DB.secret(name="host", default=environ.var())
        port: int = INI_DB.secret(name="port", default=environ.var())
        name: str = INI_DB.secret(name="dbname", default=environ.var())
        user: str = INI_DB.secret(name="user", default=environ.var())
        password: str = INI_DB.secret(name="password", default=environ.var())

    @environ.config(prefix="DB_DEV")
    class DBDevConfig:
        """
        DBConfig Class representing the configuration
        to access the database dev
        """
        host: str = DB_DEV.secret(name="host", default=environ.var())
        port: int = DB_DEV.secret(name="port", default=environ.var())
        name: str = DB_DEV.secret(name="dbname", default=environ.var())
        user: str = DB_DEV.secret(name="user", default=environ.var())
        password: str = DB_DEV.secret(name="password", default=environ.var())

    db = environ.group(DBConfig)
    db_dev = environ.group(DBDevConfig)

def getConf():
    return environ.to_config(AppConfig)
