import environ

INI_DB = environ.secrets.INISecrets.from_path_in_env("APP_DB_SECRET")
INI_EMAIL = environ.secrets.INISecrets.from_path_in_env("APP_EMAIL_SECRET")

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


    @environ.config(prefix="EMAIL")
    class EmailConfig:
        """
        DBConfig Class representing the configuration to access the database
        """
        email_from: str = INI_DB.secret(name="email_from", default=environ.var())
        email_to: str = INI_DB.secret(name="email_to", default=environ.var())
        host: str = INI_DB.secret(name="host", default=environ.var())
        
    email = environ.group(EmailConfig)
    db = environ.group(DBConfig)

def getConf():
    return environ.to_config(AppConfig)
