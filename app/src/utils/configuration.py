import logging
import os

class conf(object):
    def __init__(self, configFile):
        self.log = logging.getLogger('conf')
        logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-2s [%(filename)s:%(lineno)d] %(message)s'
                            , level=logging.INFO)
        self.configFile = configFile
        self.conf = {}
        self.loadConf()

    def getSpecific(self, key):
        return self.conf.get(key)

    def loadConf(self):
        try:
            self.log.info('Load configuration read file ' + self.configFile + '.' )
            with open(self.configFile, 'r') as f:
                for line in f.readlines():
                    if line.strip() and not line.startswith('#'):
                        k, v = line.strip().split('=', 1)
                        self.conf[k] = v
                        self.log.info('%s : %s' % ( k , v ) )
        except Exception as e:
            self.log.error('%s' % e )
