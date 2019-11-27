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
        if(self.jarName == 'JAR_POSTGRESQL'):
            self.path = os.environ.get('PATH_JAR_POSTGRESQL')
            self.driver = os.environ.get('POSTGRESQL_DRIVER')

        self.log.info('getJarConfig jarName : %s' % self.jarName)
        self.log.info('getJarConfig path    : %s' % self.path)
        self.log.info('getJarConfig driver  : %s' % self.driver)