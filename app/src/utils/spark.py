import os
import logging
from pyspark.sql import SparkSession, SQLContext, DataFrame
from utils.jar import jar

class spark(object):
    def __init__(self, appName, masterNode ):
        self.log = logging.getLogger('spark')
        logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-2s [%(filename)s:%(lineno)d] %(message)s'
                            , level=logging.INFO)
        self.appName = appName
        self.masterNode = masterNode
        self.spark = None
        self.sparkContext = None
        self.extraJar = jar('JAR_POSTGRESQL')
        self.getSparkSession()

    def getSparkSession(self):
        try:
            self.log.info('getSparkSession AppName : %s '%self.appName)
            self.spark = SparkSession.builder \
                        .master(self.masterNode) \
                        .appName(self.appName) \
                        .config("spark.driver.extraClassPath", self.extraJar.getPath()) \
                        .getOrCreate()
        except Exception as e:
            self.stopSparkSession()


    def stopSparkSession(self):
        self.log.info('Stop Spark AppName %s : '% self.appName)
        self.spark.stop()