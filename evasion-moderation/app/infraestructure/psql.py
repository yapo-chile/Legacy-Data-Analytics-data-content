from typing import Iterator, Dict, Any
import logging
import psycopg2
from infraestructure.string_iterator_io import StringIteratorIO
from infraestructure.string_iterator_io import clean_csv_value


class Database:
    """
    Class that allow do operations with postgresql database.
    """
    def __init__(self, host, port, dbname, user, password):
        self.log = logging.getLogger('psql')
        date_format = """%(asctime)s,%(msecs)d %(levelname)-2s """
        info_format = """[%(filename)s:%(lineno)d] %(message)s"""
        log_format = date_format + info_format
        logging.basicConfig(format=log_format, level=logging.INFO)
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.connection = None
        self.get_connection()

    def database_conf(self):
        """
        Method that return dict with database credentials.
        """
        return {"host": self.host,
                "port": self.port,
                "user": self.user,
                "password": self.password,
                "dbname": self.dbname}

    def get_connection(self):
        """
        Method that returns database connection.
        """
        self.log.info('get_connection DB %s/%s', self.host, self.dbname)
        self.connection = psycopg2.connect(**self.database_conf())
        self.connection.set_client_encoding('UTF-8')

    def execute_command(self, command):
        """
        Method that allow execute sql commands such as DML commands.
        """
        self.log.info('execute_command : %s',
                      command.replace('\n', ' ').replace('\t', ' '))
        cursor = self.connection.cursor()
        cursor.execute(command)
        self.connection.commit()
        cursor.close()

    def select_to_dict(self, query):
        """
        Method that from query transform raw data into dict.
        """
        self.log.info('Query : %s', query.replace(
            '\n', ' ').replace('    ', ' '))
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

    def copy_evasion(self, table_name, data_dict: Iterator[Dict[str, Any]]):
        """
        Method specific that insert data in evasion moderation table.
        """
        self.log.info('copyStringIterator init CURSOR %s.', table_name)
        with self.connection.cursor() as cursor:
            string_data = StringIteratorIO((
                '|'.join(map(clean_csv_value, (
                    row['review_order'],
                    row['pack_order'],
                    row['ifee_order'],
                    row['email'],
                    row['review_time'],
                    row['pack_start_date'],
                    row['ifee_purchase_date'],
                ))) + '\n'
                for row in data_dict
            ))
            self.log.info('Preparing data for insert.')
            cursor.copy_from(string_data, table_name, sep='|')
            self.log.info('copyStringIterator COMMIT.')
            self.connection.commit()
            self.log.info('Close cursor %s', table_name)
            cursor.close()

    def copy_evasion_det(self, table_name, data_dict: Iterator[Dict[str, Any]]):
        """
        Method specific that insert data into evasion moderation details table.
        """
        self.log.info('copyStringIterator init CURSOR %s.', table_name)
        with self.connection.cursor() as cursor:
            string_data = StringIteratorIO((
                '|'.join(map(clean_csv_value, (
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
                for row in data_dict
            ))
            self.log.info('Preparing data for insert.')
            cursor.copy_from(string_data, table_name, sep='|')
            self.log.info('copyStringIterator COMMIT.')
            self.connection.commit()
            self.log.info('Close cursor %s', table_name)
            cursor.close()

    def close_connection(self):
        """
        Method that close connection to postgresql database.
        """
        self.log.info('Close connection DB : %s/%s', self.host, self.dbname)
        self.connection.close()
