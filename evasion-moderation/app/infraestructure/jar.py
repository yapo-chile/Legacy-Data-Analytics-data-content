import os
import logging


class Jar:
    def __init__(self, jar_name):
        self.log = logging.getLogger('jar')
        date_format = """%(asctime)s,%(msecs)d %(levelname)-2s """
        info_format = """[%(filename)s:%(lineno)d] %(message)s"""
        log_format = date_format + info_format
        logging.basicConfig(format=log_format, level=logging.INFO)
        self.jar_name = jar_name
        self.path = None
        self.driver = None
        self.get_jar_config()

    def get_driver(self):
        """
        Method that return driver from jar
        """
        return self.driver

    def get_path(self):
        """
        Method that return full path from jar file.
        """
        return self.path

    def get_jar_config(self):
        if self.jar_name == 'JAR_POSTGRESQL':
            self.path = os.environ.get('PATH_JAR_POSTGRESQL')
            self.driver = os.environ.get('POSTGRESQL_DRIVER')

        self.log.info('getJarConfig jar_name : %s', self.jar_name)
        self.log.info('getJarConfig path    : %s', self.path)
        self.log.info('getJarConfig driver  : %s', self.driver)
