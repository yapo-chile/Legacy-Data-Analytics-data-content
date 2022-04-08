import environ

INI_DB = environ.secrets.INISecrets.from_path_in_env("APP_DB_SECRET")
INI_STATS = environ.secrets.INISecrets.from_path_in_env("APP_STATISTICS_SECRET")


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

    @environ.config(prefix="STATISTICS")
    class StatisticDatabaseConfig:
        host: str = INI_STATS.secret(name="host", default=environ.var())
        port: int = INI_STATS.secret(name="port", default=environ.var())
        name: str = INI_STATS.secret(name="dbname", default=environ.var())
        user: str = INI_STATS.secret(name="user", default=environ.var())
        password: str = INI_STATS.secret(name="password", default=environ.var())

    database = environ.group(DBConfig)
    stat_database = environ.group(StatisticDatabaseConfig)

def getConf():
    return environ.to_config(AppConfig)
