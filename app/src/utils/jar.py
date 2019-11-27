import os
import logging

class jar(object):
    def __init__(self, jarName ):
        self.log = logging.getLogger('jar')
        logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-2s [%(filename)s:%(lineno)d] %(message)s'
                            , level=logging.INFO)
        self.jarName = jarName
        self.path = None
        self.driver = None
        self.getJarConfig()

    def getDriver(self):
        return self.driver

    def getPath(self):
        return self.path

    def getJarConfig(self):
        self.log.info('getJarConfig : %s '%self.jarName)
        if(self.jarName == 'JAR_POSTGRESQL'):
            self.path = os.environ.get('PATH_JAR_POSTGRESQL')
            self.driver = os.environ.get('POSTGRESQL_DRIVER')