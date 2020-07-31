# pylint: disable=no-member
# utf-8
import logging
import pandas as pd
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams

class RetentionSellerPacks():

    def __init__(self, config, params: ReadParams) -> None:
        self.config = config
        self.params = params
        self.logger = logging.getLogger('Retention-seller-pack')
        self.db_dev = None
        self.db = None

    def delete_from_retention_seller_packs(self):
        """
        Method that execute delete query that delete info with
        month_id corresponding to date_from %Y%m
        from table stg.retention_sellers_packs
        """
        query = Query(self.config, self.params)
        if self.db_dev is None:
            self.db_dev = Database(conf=self.config.db_dev)
        self.logger.info('Executing query delete')
        self.db_dev.execute_command(query.delete_retention_sellers_packs())
        self.logger.info('Query executed')


    def save_retention_seller_packs(self) -> None:
        """
        Method that insert data_retention_seller_packs dataframe
        in stg.retention_sellers_packs
        """
        query = Query(self.config, self.params)
        if self.db_dev is None:
            self.db_dev = Database(conf=self.config.db_dev)
        self.data_retention_seller_packs.to_sql(
            query.table_dest_retention_seller_pack,
            con=self.db_dev)

    @property
    def data_retention_seller_packs(self):
        """
        Getter from data of retention seller packs
        """
        return self.__data_retention_seller_packs

    @data_retention_seller_packs.setter
    def data_retention_seller_packs(self, config):
        """
        Setter of data of retention seller packs
        Get data from sql dws executing query's
        """
        query = Query(config, self.params)
        if self.db is None:
            self.db = Database(conf=config.db)
        self.logger.info('Ejecutando query')
        data = pd.read_sql(sql=query.query_retention_seller_pack(),
                           con=self.db.connection)
        self.logger.info('Query terminada')
        self.__data_retention_seller_packs = data
        self.db.close_connection()

    def generate(self):
        self.logger.info('Obteniendo Datos')
        self.data_retention_seller_packs = self.config
        self.logger.info('Eliminando datos anteriores')
        self.delete_from_retention_seller_packs()
        self.logger.info('Guardando datos')
        self.save_retention_seller_packs()
