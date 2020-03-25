import logging
import psycopg2
import psycopg2.extras
import pandas as pd


class Database:
    """
    Class that allow do operations with postgresql database.
    """
    def __init__(self, conf) -> None:
        self.log = logging.getLogger('psql')
        date_format = """%(asctime)s,%(msecs)d %(levelname)-2s """
        info_format = """[%(filename)s:%(lineno)d] %(message)s"""
        log_format = date_format + info_format
        logging.basicConfig(format=log_format, level=logging.INFO)
        self.conf = conf
        self.connection = None
        self.get_connection()

    def database_conf(self) -> dict:
        """
        Method that return dict with database credentials.
        """
        return {"host": self.conf.host,
                "port": self.conf.port,
                "user": self.conf.user,
                "password": self.conf.password,
                "dbname": self.conf.name}

    def get_connection(self) -> None:
        """
        Method that returns database connection.
        """
        self.log.info('get_connection DB %s/%s', self.conf.host, self.conf.name)
        self.connection = psycopg2.connect(**self.database_conf())
        self.connection.set_client_encoding('UTF-8')

    def execute_command(self, command) -> None:
        """
        Method that allow execute sql commands such as DML commands.
        """
        self.log.info('execute_command : %s',
                      command.replace('\n', ' ').replace('\t', ' '))
        cursor = self.connection.cursor()
        cursor.execute(command)
        self.connection.commit()
        cursor.close()

    def select_to_dict(self, query) -> pd.DataFrame:
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
        pd_result = pd.DataFrame(result)
        cursor.close()
        return pd_result

    def insert_data_pivot(self, data_dict: pd.DataFrame) -> None:
        self.log.info('INSERT INTO %s', self.conf.table_pivot)
        page_size: int = 10000
        with self.connection.cursor() as cursor:
            psycopg2.extras \
                .execute_values(cursor,
                                """ INSERT INTO """ + self.conf.table_pivot +
                                """ ( pack_id,
                                      account_id,
                                      category,
                                      date_start,
                                      date_end,
                                      days,
                                      slots,
                                      product_id,
                                      price,
                                      doc_num,
                                      tipo_pack,
                                      email
                                    )
                                    VALUES %s; """, ((
                                        row.pack_id,
                                        row.account_id,
                                        row.category,
                                        row.date_start,
                                        row.date_end,
                                        row.days,
                                        row.slots,
                                        row.product_id,
                                        row.price,
                                        row.doc_num,
                                        row.tipo_pack,
                                        row.email
                                    ) for row in data_dict.itertuples()),
                                page_size=page_size)
            self.log.info('INSERT %s COMMIT.', self.conf.table_pivot)
            self.connection.commit()
            self.log.info('CLOSE CURSOR %s', self.conf.table_pivot)
            cursor.close()

    def insert_data_stg(self, data_dict: pd.DataFrame) -> None:
        self.log.info('INSERT INTO %s', self.conf.table_stg)
        page_size: int = 10000
        with self.connection.cursor() as cursor:
            psycopg2.extras \
                .execute_values(cursor,
                                """ INSERT INTO """ + self.conf.table_stg +
                                """ ( 
                                      account_id,
                                      category,
                                      date_start,
                                      date_end,
                                      days,
                                      slots,
                                      product_id,
                                      price,
                                      doc_num,
                                      tipo_pack,
                                      email
                                    )
                                    VALUES %s; """, ((
                                        row.account_id,
                                        row.category,
                                        row.date_start,
                                        row.date_end,
                                        row.days,
                                        row.slots,
                                        row.product_id,
                                        row.price,
                                        row.doc_num,
                                        row.tipo_pack,
                                        row.email
                                    ) for row in data_dict.itertuples()),
                                page_size=page_size)
            self.log.info('INSERT %s COMMIT.', self.conf.table_stg)
            self.connection.commit()
            self.log.info('CLOSE CURSOR %s', self.conf.table_stg)
            cursor.close()


    def close_connection(self):
        """
        Method that close connection to postgresql database.
        """
        self.log.info('Close connection DB : %s/%s',
                      self.conf.host,
                      self.conf.name)
        self.connection.close()
