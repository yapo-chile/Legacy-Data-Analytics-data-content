import logging
from pyspark.sql import SparkSession, SQLContext
from infraestructure.jar import _jar


class Spark:
    """
    Class _spark that allow connect pyspark interfaces.
    """
    def __init__(self, app_name, node):
        self.log = logging.getLogger('spark')
        date_format = """%(asctime)s,%(msecs)d %(levelname)-2s """
        info_format = """[%(filename)s:%(lineno)d] %(message)s"""
        log_format = date_format + info_format
        logging.basicConfig(format=log_format, level=logging.INFO)
        self.app_name = app_name
        self.node = node
        self.spark = None
        self.spark_context = None
        self.extra_jar = _jar('JAR_POSTGRESQL')
        self.get_spark_session()

    def get_spark_session(self):
        """
        Method that init spark session.
        """
        try:
            self.log.info('get_spark_session app_name : %s', self.app_name)
            self.spark = SparkSession.builder \
                .master(self.node) \
                .appName(self.app_name) \
                .config("spark.driver.extraClassPath", self.extra_jar.getPath()) \
                .getOrCreate()

            self.spark_context = self.spark.spark_context
        except Exception as ex:
            self.log.error('%s', ex)
            self.stop_spark_session()

    def get_spark_sql(self, url_connect, user, password, query):
        """
        Method that allow do query to postgresql database.
        """
        try:
            url = 'jdbc:postgresql://' + url_connect
            sql_context = SQLContext(self.spark_context)

            data_frame = sql_context.read.format('jdbc') \
                .option('driver', self.extra_jar.getDriver()) \
                .option('url', url) \
                .option('dbtable', query) \
                .option('user', user) \
                .option('password', password) \
                .load()
            return data_frame
        except Exception as ex:
            self.log.error('%s', ex)
            self.stop_spark_session()

    def stop_spark_session(self):
        """
        Method that stop SparkSession and SparkContext
        """
        self.log.info('Stop Spark AppName : %s', self.app_name)
        self.spark.stop()
