import os
import environ


INI_SOURCEDB = environ.secrets.INISecrets.from_path_in_env("APP_SOURCEDB")
INI_ENDPOINTDB = environ.secrets.INISecrets.from_path_in_env("APP_ENDPOINTDB")


@environ.config(prefix="APP")
class AppConfig:
    """
    AppConfig Class representing the configuration of the application
    """

    @environ.config(prefix="SOURCEDB")
    class SourceDBConfig:
        """
        SourceDBConfig Class representing the configuration to access the database
        """
        host: str = INI_SOURCEDB.secret(name="host", default=None)
        port: int = INI_SOURCEDB.secret(name="port", default=None)
        name: str = INI_SOURCEDB.secret(name="dbname", default=None)
        user: str = INI_SOURCEDB.secret(name="user", default=None)
        password: str = INI_SOURCEDB.secret(name="password", default=None)
        if host is None:
            host = os.environ.get("SOURCEDB_HOST")
        if port is None:
            port = os.environ.get("SOURCEDB_PORT")
        if name is None:
            name = os.environ.get("SOURCEDB_NAME")
        if user is None:
            user = os.environ.get("SOURCEDB_USER")
        if password is None:
            password = os.environ.get("SOURCEDB_PASSWORD")


    @environ.config(prefix="ENDPOINTDB")
    class EndpointDBConfig:
        """
        EndpointDBConfig Class representing the configuration to access the database
        """
        host: str = INI_ENDPOINTDB.secret(name="host", default=None)
        port: int = INI_ENDPOINTDB.secret(name="port", default=None)
        name: str = INI_ENDPOINTDB.secret(name="dbname", default=None)
        user: str = INI_ENDPOINTDB.secret(name="user", default=None)
        password: str = INI_ENDPOINTDB.secret(name="password", default=None)
        table_em: str = environ.var("dm_analysis.moderacion_evasion")
        table_emd: str = environ.var("dm_analysis.moderacion_evasion_detalles")
        if host is None:
            host = os.environ.get("ENDPOINTDB_HOST")
        if port is None:
            port = os.environ.get("ENDPOINTDB_PORT")
        if name is None:
            name = os.environ.get("ENDPOINTDB_NAME")
        if user is None:
            user = os.environ.get("ENDPOINTDB_USER")
        if password is None:
            password = os.environ.get("ENDPOINTDB_PASSWORD")
    sourcedb = environ.group(SourceDBConfig)
    endpointdb = environ.group(EndpointDBConfig)
