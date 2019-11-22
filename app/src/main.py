import sys
import logging
from utils.readParams import readParams
from utils.configuration import conf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *

def getSparkSql(sparkContext, query):
    url="jdbc:postgresql://" + configuration.getSpecific('BLOCKETDB.HOST') + ":" + configuration.getSpecific('BLOCKETDB.PORT') + "/" + configuration.getSpecific('BLOCKETDB.DATABASE')
    sparkContext.read.format('jdbc') \
            .option('url', url ) \
            .option('dbtable', query ) \
            .option('user', configuration.getSpecific('BLOCKETDB.USERNAME') ) \
            .option('password', configuration.getSpecific('BLOCKETDB.PASSWORD') ) \
            .load()

def getSparkSession(node='local', appName='mainSpark', jar=None):
    logger.info('sparkSession appName : %s' % appName)
    logger.info('')
    spark = SparkSession.builder \
            .master(node) \
            .config("spark.driver.extraClassPath", jar) \
            .getOrCreate()

    sparkContext = spark.sparkContext

if __name__ == '__main__':
    logger = logging.getLogger('content-evasion-moderation')
    logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-2s [%(filename)s:%(lineno)d] %(message)s'
                            , level=logging.INFO)
    params = readParams(sys.argv)
    logger.info('%s' %params.getDate1())
    
    configuration = conf(params.getConfigurationFile())
    getSparkSession(params.getAppName())

