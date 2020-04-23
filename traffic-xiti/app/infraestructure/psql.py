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

    def insert_data_traffic(self, data_dict: pd.DataFrame) -> None:
        self.log.info('INSERT INTO %s', self.conf.table_traffic)
        page_size: int = 10000
        with self.connection.cursor() as cursor:
            psycopg2.extras \
                .execute_values(cursor,
                                """ INSERT INTO """ + self.conf.table_traffic +
                                """ ( timedate,
                                      vertical,
                                      platform,
                                      dau_xiti,
                                      visits_xiti,
                                      leads_xiti,
                                      dau_pulse,
                                      visits_pulse,
                                      browsers_pulse,
                                      buyers_pulse,
                                      unique_leads_pulse
                                    )
                                    VALUES %s; """, ((
                                        row.timedate,
                                        row.vertical,
                                        row.platform,
                                        row.dau_xiti,
                                        row.visits_xiti,
                                        row.leads_xiti,
                                        row.dau_pulse,
                                        row.visits_pulse,
                                        row.browsers_pulse,
                                        row.buyers_pulse,
                                        row.unique_leads_pulse
                                    ) for row in data_dict.itertuples()),
                                page_size=page_size)
            self.log.info('INSERT %s COMMIT.', self.conf.table_traffic)
            self.connection.commit()
            self.log.info('CLOSE CURSOR %s', self.conf.table_traffic)
            cursor.close()

    def insert_data_leads_per_user(self, data_dict: pd.DataFrame) -> None:
        self.log.info('INSERT INTO %s', self.conf.table_leads_per_user)
        page_size: int = 10000
        with self.connection.cursor() as cursor:
            psycopg2.extras \
                .execute_values(cursor,
                                """ INSERT INTO """ + \
                                    self.conf.table_leads_per_user +
                                """ ( timedate,
                                      vertical,
                                      platform,
                                      leads_per_user,
                                      ads_with_lead_per_user
                                    )
                                    VALUES %s; """, ((
                                        row.timedate,
                                        row.vertical,
                                        row.platform,
                                        row.leads_per_user,
                                        row.ads_with_lead_per_user
                                    ) for row in data_dict.itertuples()),
                                page_size=page_size)
            self.log.info('INSERT %s COMMIT.', self.conf.table_leads_per_user)
            self.connection.commit()
            self.log.info('CLOSE CURSOR %s', self.conf.table_leads_per_user)
            cursor.close()

    def insert_data_liquidity(self, data_dict: pd.DataFrame) -> None:
        self.log.info('INSERT INTO %s', self.conf.table_liquidity)
        page_size: int = 10000
        with self.connection.cursor() as cursor:
            psycopg2.extras \
                .execute_values(cursor,
                                """ INSERT INTO """ + \
                                    self.conf.table_liquidity +
                                """ ( timedate,
                                      vertical,
                                      ad_with_lead_7days
                                    )
                                    VALUES %s; """, ((
                                        row.timedate,
                                        row.vertical,
                                        row.ad_with_lead_7days
                                    ) for row in data_dict.itertuples()),
                                page_size=page_size)
            self.log.info('INSERT %s COMMIT.', self.conf.table_liquidity)
            self.connection.commit()
            self.log.info('CLOSE CURSOR %s', self.conf.table_liquidity)
            cursor.close()

    def close_connection(self):
        """
        Method that close connection to postgresql database.
        """
        self.log.info('Close connection DB : %s/%s',
                      self.conf.host,
                      self.conf.name)
        self.connection.close()
