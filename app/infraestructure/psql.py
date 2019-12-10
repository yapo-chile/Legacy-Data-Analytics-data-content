from typing import Iterator, Dict, Any, Optional
import logging
import psycopg2
from infraestructure.stringIteratorIO import StringIteratorIO
from infraestructure.stringIteratorIO import cleanCsvValue
from infraestructure.stringIteratorIO import cleanStrValue


class database(object):
    def __init__(self, host, port, dbname, user, password):
        self.log = logging.getLogger('psql')
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
        self.connection.set_client_encoding('UTF-8')

    def executeCommand(self, command):
        self.log.info('executeCommand : %s'% command.replace('\n', ' ').replace('\t', ' '))
        cursor = self.connection.cursor()
        cursor.execute(command)
        self.connection.commit()
        cursor.close()


    def selectToDict(self, query):
        self.log.info('Query : %s' % query.replace('\n', ' ').replace('    ', ' '))
        cursor = self.connection.cursor()
        cursor.execute(query)
        fieldnames = [name[0] for name in cursor.description]
        result = []
        for row in cursor.fetchall():
            rowset = []
            for field in zip(fieldnames, row):
                rowset.append(field)
            result.append(dict(rowset))
        cursor.close()
        return result

    def copyEvasion(self, tableName, dataDict: Iterator[Dict[str, Any]]):
        self.log.info('copyStringIterator init CURSOR %s.' % tableName)
        with self.connection.cursor() as cursor:
            stringData = StringIteratorIO((
                '|'.join(map(cleanCsvValue, (
                    rowDict['review_order'],
                    rowDict['pack_order'],
                    rowDict['ifee_order'],
                    rowDict['email'],
                    rowDict['review_time'],
                    rowDict['pack_start_date'],
                    rowDict['ifee_purchase_date'],
                ))) + '\n'
                for rowDict in dataDict
            ))
            self.log.info('Preparing data for insert.')
            cursor.copy_from(stringData, tableName, sep='|')
            self.log.info('copyStringIterator COMMIT.')
            self.connection.commit()
            self.log.info('Close cursor %s' % tableName)
            cursor.close()


    def copyEvasionDet(self, tableName, dataDict: Iterator[Dict[str, Any]]):
        self.log.info('copyStringIterator init CURSOR %s.' % tableName)
        with self.connection.cursor() as cursor:
            stringData = StringIteratorIO((
                '|'.join(map(cleanCsvValue, (
                    row['review_order'],
                    row['email'],
                    row['ad_id'],
                    row['admin_name'],
                    row['review_time'],
                    row['queue'],
                    row['refusal_reason_text'],
                    row['pack_id'],
                    row['account_id'],
                    row['type'],
                    row['slots'],
                    row['date_start'],
                    row['date_end'],
                    row['product_name'],
                    row['tipo_pack'],
                    row['ifee_ad_id'],
                    row['ifee_name'],
                    row['ifee_purchase_date'],
                    row['ifee_price'],
                ))) + '\n'
                for row in dataDict
            ))
            self.log.info('Preparing data for insert.')
            cursor.copy_from(stringData, tableName, sep='|')
            self.log.info('copyStringIterator COMMIT.')
            self.connection.commit()
            self.log.info('Close cursor %s' % tableName)
            cursor.close()

    def closeConnection(self):
        self.connection.close()