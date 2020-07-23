import environ

INI_DB = environ.secrets.INISecrets.from_path_in_env("APP_DB_SECRET")
INI_AWS_ENV_VAR = environ.secrets.INISecrets.from_path_in_env("APP_AWS_SECRET")

@environ.config(prefix="APP")
class AppConfig:
    """
    AppConfig Class representing the configuration of the application
    """
    @environ.config(prefix="AWS")
    class AWSConfig:
        """
        AWSConfig Class representing the configuration to access the
        aws services with boto
        """
        access_key_id: str = INI_AWS_ENV_VAR\
            .secret(name="aws_access_key_id", default=environ.var())
        secret_access_key: str = INI_AWS_ENV_VAR.\
            secret(name="aws_secret_access_key", default=environ.var())

    @environ.config(prefix="DB")
    class DBConfig:
        """
        DBConfig Class representing the configuration to access the
        datawarehouse database
        """
        host: str = INI_DB.secret(name="host", default=environ.var())
        port: int = INI_DB.secret(name="port", default=environ.var())
        name: str = INI_DB.secret(name="dbname", default=environ.var())
        user: str = INI_DB.secret(name="user", default=environ.var())
        password: str = INI_DB.secret(name="password", default=environ.var())

    db = environ.group(DBConfig)
    aws = environ.group(AWSConfig)

def getConf():
    return environ.to_config(AppConfig)
