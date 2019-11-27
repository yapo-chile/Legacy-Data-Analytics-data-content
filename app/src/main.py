import sys
import logging
from utils.readParams import readParams
from utils.configuration import conf
from utils.spark import spark


if __name__ == '__main__':
    logger = logging.getLogger('content-evasion-moderation')
    logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-2s [%(filename)s:%(lineno)d] %(message)s'
                            , level=logging.INFO)
    params = readParams(sys.argv)
        
    configuration = conf(params.getConfigurationFile())
    appName='content-evasion-moderation-child'

    logger.info('Spark process.')
    spark = spark(appName, params.getMaster())
    spark.stopSparkSession()
    logger.info('Process ended successed.')

