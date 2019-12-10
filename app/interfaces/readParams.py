import sys
import logging
import datetime
import os
from datetime import timedelta


class readParams(object):
    def __init__(self, strParseParams):
        self.strParseParams = strParseParams
        self.dateFrom = None
        self.dateTo = None
        self.env = None
        self.appName = None
        self.currentYear = None
        self.lastYear = None
        self.configurationFile = None
        self.jarPsql = None
        self.master = None
        self.logger = logging.getLogger('readParams')
        logging.basicConfig(
            format='%(asctime)s,%(msecs)d %(levelname)-2s [%(filename)s:%(lineno)d] %(message)s', level=logging.INFO)
        self.loadParams()
        self.validateParams()

    def getDateFrom(self) -> str:
        return self.dateFrom

    def getDateTo(self) -> str:
        return self.dateTo

    def getEnv(self) -> str:
        return self.env

    def getAppName(self) -> str:
        return self.appName

    def getCurrentYear(self) -> str:
        return self.currentYear

    def getLastYear(self) -> str:
        return self.lastYear

    def getConfigurationFile(self) -> str:
        return self.configurationFile

    def getJarPsql(self) -> str:
        return self.jarPsql

    def getMaster(self):
        return self.master

    def setDateFrom(self, dateFrom):
        self.dateFrom = dateFrom

    def setDateTo(self, dateTo):
        self.date2 = dateTo

    def setEnv(self, env):
        self.env = env

    def setAppName(self, appName):
        self.appName = appName

    def setCurrentYear(self, currentYear):
        self.currentYear = currentYear

    def setLastYear(self, lastYear):
        self.lastYear = lastYear

    def loadParams(self) -> None:
        """
        Method [ load_params ] is method that load params into each attribute.
        """
        try:
            self.logger.info('Python name : %s ' % self.strParseParams[0])
            for i in range(1, len(self.strParseParams)):
                self.logger.info('Param[%s] : %s ' %
                                 (i, self.strParseParams[i]))
                param = self.strParseParams[i].split("=")
                self.mappingParams(param[0], param[1])
        except Exception as e:
            self.logger.error('%s' % e)
            exit()

    def mappingParams(self, key: str, value: str) -> None:
        """
        Method [ mapping_params ] is method that join attribute with key.
        Param  [ key ] is the key that be compare with params define for assign to attribute.
        Param  [ value ] is value that will be assign to attribute.
        """
        try:
            if (key == '-dateFrom'):
                self.dateFrom = value
                self.currentYear = value[0:4]
                year_int = int(self.currentYear)
                year_int = year_int - 1
                self.lastYear = str(year_int)
            elif (key == '-dateTo'):
                self.dateTo = value
            elif (key == '-env'):
                self.env = value
            elif (key == '-appName'):
                self.appName = value
            elif (key == '-jarPsql'):
                self.jarPsql = value
            elif (key == '-master'):
                self.master = value
        except Exception as e:
            self.logger.error('%s' % e)
            exit()

    def validateParams(self):
        """
        Method [ validate_params ] is method validate that each attribute have assign a value.
        """
        try:
            self.logger.info('Validate params.')
            current_date = datetime.datetime.now()
            if (self.dateFrom is None):
                temp_date = current_date + timedelta(days=-1)
                currentYear = temp_date.year
                lastYear = temp_date.year - 1
                self.currentYear = str(currentYear)
                self.lastYear = str(lastYear)
                self.dateFrom = temp_date.strftime('%Y-%m-%d')
            if (self.dateTo is None):
                temp_date = current_date + timedelta(days=-1)
                self.dateTo = temp_date.strftime('%Y-%m-%d')
            if (self.env is None):
                self.env = 'prod'
            if(self.appName is None):
                path_appName = self.strParseParams[0].split("/")
                appName = path_appName[len(path_appName)-1].split(".")
                self.appName = appName[0]
            if(self.jarPsql is None):
                self.jarPsql = os.environ.get("PATH_JAR_POSTGRESQL")
            if(self.master is None):
                self.master = 'local'
            if(self.configurationFile is None):
                self.configurationFile = os.environ.get("CONFIGURATION_FILE")

            self.logger.info('App Name  : %s' % self.appName)
            self.logger.info('Date from : %s' % self.dateFrom)
            self.logger.info('Date to   : %s' % self.dateTo)
            self.logger.info('Env       : %s' % self.env)
            self.logger.info('Current year : %s' % self.currentYear)
            self.logger.info('Last year : %s' % self.lastYear)
            self.logger.info('Config file : %s' % self.configurationFile)
            self.logger.info('Jar psql : %s' % self.jarPsql)
            self.logger.info('Node : %s' % self.master)

        except Exception as e:
            self.logger.error('%s' % e)
            exit()
