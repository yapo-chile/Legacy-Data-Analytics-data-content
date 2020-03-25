import environ
import os

INI_DB = environ.secrets.INISecrets.from_path_in_env("APP_DB_SECRET")


@environ.config(prefix="APP")
class AppConfig:
    """
    AppConfig Class representing the configuration of the application
    """

    @environ.config(prefix="CSV")
    class CsvConfig:
        """
        DBConfig Class representing the configuration to access the database
        """
        path: str = os.environ.get("CSV_FILE")

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
        table_pivot: str = environ.var("dm_analysis.packs_pivot")
        table_stg: str = environ.var("stg.packs")
    db = environ.group(DBConfig)
    csv = environ.group(CsvConfig)


def getConf():
    return environ.to_config(AppConfig)
