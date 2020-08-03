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
        self.month_id = self.params.date_from.strftime('%Y%m')
        self.db = None

    def delete_from_retention_seller_packs(self):
        """
        Method that execute delete query that delete info with
        month_id corresponding to date_from %Y%m
        from table stg.retention_sellers_packs
        """
        query = Query(self.config, self.params)
        if self.db is None:
            self.db = Database(conf=self.config.db)
        self.logger.info('Executing query delete')
        self.db.execute_command(
            query.delete_retention_sellers_packs(self.month_id))
        self.logger.info('Query executed')


    def save_retention_seller_packs(self) -> None:
        """
        Method that insert data_retention_seller_packs dataframe
        in stg.retention_sellers_packs
        """
        query = Query(self.config, self.params)
        if self.db is None:
            self.db = Database(conf=self.config.db)
        n_new_regs = len(self.data_retention_seller_packs.index)
        self.logger.info('Inserting new data, {num_regs} new registers'\
            .format(num_regs=n_new_regs))
        self.logger.info('Guardando datos')
        self.db.insert_data(
            table_name=query.table_dest_rsp,
            data=self.data_retention_seller_packs)
        self.db.close_connection()

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
        self.logger.info('Ejecutando query - Get Data')
        data = pd.read_sql(sql=query.query_retention_seller_pack(),
                           con=self.db.connection)
        self.logger.info('Query ended')
        self.__data_retention_seller_packs = data

    def generate(self):
        self.data_retention_seller_packs = self.config
        self.delete_from_retention_seller_packs()
        self.save_retention_seller_packs()
