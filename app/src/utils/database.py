import logging
import psycopg2


class database(object):
    def __init__(self, host, port, dbname, user, password):
        self.log = logging.getLogger('content-evasion-moderation')
        format = """%(asctime)s,%(msecs)d %(levelname)-2s [%(filename)s:%(lineno)d] %(message)s"""
        logging.basicConfig(format=format,
                        level=logging.INFO)
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.connection = None
        self.getConnection()
    
    def DatabaseConf(self):
        return {"host": self.host,
                "port": self.port,
                "user": self.user,
                "password": self.password,
                "dbname": self.dbname}
    
    def getConnection(self):
        self.log.info('getConnection DB %s/%s'% (self.host, self.dbname))
        self.connection = psycopg2.connect(**self.DatabaseConf())

    def executeCommand(self, command):
        self.log.info('executeCommand : %s'% command)
        cursor = self.connection.cursor()
        cursor.execute(command)
        self.connection.commit()
        cursor.close()

    def closeConnection(self):
        self.connection.close()