import sys
import logging
import datetime
from datetime import timedelta

class readParams(object):
    #Constructor
    def __init__(self, strParseParams ):
        """
        Method    [ __init__ ] is constructor of read_params class.
        Attribute [ strParseParams ][ String Array ] is array string that contains params of execution . For example, python mypy.py -d1=2019-08-02 -d2=2019-08-02 -env=dev -appName=mypython
        Attribute [ date1 ][ String ] is date1 variable. For example DW_BLOCKETDB.HOST.
        Attribute [ date2 ][ String ] is date2 variable. For example dw_blocketdb_ch.
        Attribute [ env ][ String ] is enviroment type. For example [ dev | qa | prod ].
        Attribute [ appName ][ String ] is a appName for sparkSession. For example mypython.
        Attribute [ currentYear ][ String ] is current year of execution. For example 2019.
        Attribute [ lastYear ][ String ] is last year of execution. For example 2018.
        Attribute [ logger ][ logging ] is logging object that allow create log.
        """
        self.strParseParams = strParseParams
        self.date1 = ""
        self.date2 = ""
        self.env = ""
        self.appName = ""
        self.currentYear = ""
        self.lastYear = ""
        self.configurationFile = ""
        self.logger =  logging.getLogger('readParams')
        logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-2s [%(filename)s:%(lineno)d] %(message)s'
                            , level=logging.INFO)
        self.loadParams()
        self.validateParams()

    #Getter and Setter attributes
    def getDate1(self):
        return self.date1

    def getDate2(self):
        return self.date2

    def getEnv(self):
        return self.env

    def getAppName(self):
        return self.appName

    def getCurrentYear(self):
        return self.currentYear

    def getLastYear(self):
        return self.lastYear
    
    def getConfigurationFile(self):
        return self.configurationFile

    def setDate1( self, date1 ):
        self.date1 = date1

    def setDate2( self, date2 ):
        self.date2 = date2

    def setEnv( self, env ):
        self.env = env

    def setAppName( self, appName ):
        self.appName = appName

    def setCurrentYear( self, currentYear ):
        self.currentYear = currentYear

    def set_lastYear( self, lastYear ):
        self.lastYear = lastYear

    def loadParams( self ):
        """
        Method [ load_params ] is method that load params into each attribute.
        """
        try:
            self.logger.info( 'Python name : %s ' % self.strParseParams[0] )
            for i in range(1, len(self.strParseParams ) ):
                self.logger.info( 'Param[%s] : %s ' % ( i, self.strParseParams[ i ] ) )
                param = self.strParseParams[ i ].split("=")
                self.mappingParams( param[0], param[1] )
        except Exception as e:
            self.logger.error('%s' % e )
            exit()

    def mappingParams(self, key, value ):
        """
        Method [ mapping_params ] is method that join attribute with key.
        Param  [ key ] is the key that be compare with params define for assign to attribute.
        Param  [ value ] is value that will be assign to attribute.
        """
        try:
            if ( key == '-d1' ):
                self.date1 = value
                self.currentYear = value[0:4]
                year_int = int( self.currentYear )
                year_int = year_int - 1
                self.lastYear = year_int
            elif ( key == '-d2' ):
                self.date2 = value
            elif ( key == '-env' ):
                self.env = value
            elif ( key == '-appName' ):
                self.appName = value
            elif ( key == '-configFile' ):
                self.configurationFile = value
        except Exception as e:
            self.logger.error('%s' % e )
            exit()

    def validateParams(self):
        """
        Method [ validate_params ] is method validate that each attribute have assign a value.
        """
        try:
            self.logger.info('Validate params.')
            current_date = datetime.datetime.now()
            if ( self.date1 == '' ):
                temp_date = current_date + timedelta(days=-1)
                currentYear = temp_date.year
                lastYear = temp_date.year - 1
                self.currentYear = str( currentYear )
                self.lastYear = str( lastYear )
                self.date1 = temp_date.strftime('%Y-%m-%d')
            if ( self.date2 == '' ):
                temp_date = current_date + timedelta(days=-1)
                self.date2 = temp_date.strftime('%Y-%m-%d')
            if ( self.env == '' ):
                self.env = 'prod'
            if( self.appName == '' ):
                path_appName = self.strParseParams[ 0 ].split("/")
                appName = path_appName[len(path_appName)-1].split(".")
                self.appName = appName[0]

            self.logger.info('App Name     : %s' % self.appName)
            self.logger.info('Date 1       : %s' % self.date1)
            self.logger.info('Date 2       : %s' % self.date2)
            self.logger.info('Env          : %s' % self.env)
            self.logger.info('Current Year : %s' % self.currentYear)
            self.logger.info('Last    Year : %s' % self.lastYear)

        except Exception as e:
            self.logger.error('%s' % e)
            exit()