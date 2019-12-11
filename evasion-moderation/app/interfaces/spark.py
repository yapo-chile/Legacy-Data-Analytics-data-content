import os
import logging
from pyspark.sql import SparkSession, SQLContext, DataFrame
from infraestructure.jar import jar


class spark(object):
    def __init__(self, appName, masterNode):
        self.log = logging.getLogger('spark')
        logging.basicConfig(
            format='%(asctime)s,%(msecs)d %(levelname)-2s [%(filename)s:%(lineno)d] %(message)s', level=logging.INFO)
        self.appName = appName
        self.masterNode = masterNode
        self.spark = None
        self.sparkContext = None
        self.extraJar = jar('JAR_POSTGRESQL')
        self.getSparkSession()

    def getSparkSession(self):
        try:
            self.log.info('getSparkSession AppName : %s ' % self.appName)
            self.spark = SparkSession.builder \
                .master(self.masterNode) \
                .appName(self.appName) \
                .config("spark.driver.extraClassPath", self.extraJar.getPath()) \
                .getOrCreate()

            self.sparkContext = self.spark.sparkContext
        except Exception as e:
            self.log.error('%s' % e)
            self.stopSparkSession()

    def getSparkSql(self, host, port, dbname, user, password, query):
        try:
            url = 'jdbc:postgresql://' + host + ":" + port + "/" + dbname
            sqlContext = SQLContext(self.sparkContext)

            dataFrame = sqlContext.read.format('jdbc') \
                .option('driver', self.extraJar.getDriver()) \
                .option('url', url) \
                .option('dbtable', query) \
                .option('user', user) \
                .option('password', password) \
                .load()
            return dataFrame
        except Exception as e:
            self.log.error('%s' % e)
            self.stopSparkSession()

    def stopSparkSession(self):
        self.log.info('Stop Spark AppName : %s' % self.appName)
        self.spark.stop()
