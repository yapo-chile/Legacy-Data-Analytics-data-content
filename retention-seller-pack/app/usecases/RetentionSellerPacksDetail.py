# pylint: disable=no-member
# utf-8
import logging
import pandas as pd
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams

class RetentionSellerPacksDetail():

    def __init__(self, config, params: ReadParams) -> None:
        self.config = config
        self.params = params
        self.logger = logging.getLogger('Retention-seller-pack-detail')
        self.month_id = self.params.date_from.strftime('%Y%m')
        self.db = None
        self.db_dev = None

    def delete_from_retention_seller_packs_detail(self):
        """
        Method that delete data from month in process if exist
        """
        query = Query(self.config, self.params)
        if self.db_dev is None:
            self.db_dev = Database(conf=self.config.db_dev)
        self.logger.info('Executing query delete')
        self.db_dev.execute_command(
            query.delete_retention_sellers_packs(self.month_id))
        self.logger.info('Query executed')

    def save_retention_seller_packs_detail(self) -> None:
        """
        Method that save data in datawarehouse in table
        retention seller packs detail
        """
        query = Query(self.config, self.params)
        if self.db_dev is None:
            self.db_dev = Database(conf=self.config.db_dev)
        n_new_regs = len(self.data_retention_seller_packs_detail.index)
        self.logger.info('Inserting new data, {num_regs} new registers'\
            .format(num_regs=n_new_regs))
        self.data_retention_seller_packs_detail.to_sql(
            query.table_dest_retention_seller_pack,
            con=self.db_dev)
        self.db_dev.close_connection()

    @property
    def data_retention_seller_packs_detail(self):
        return self.__data_retention_seller_packs_detail

    @data_retention_seller_packs_detail.setter
    def data_retention_seller_packs_detail(self, config):
        query = Query(config, self.params)
        if self.db is None:
            self.db = Database(conf=config.db)
        self.logger.info('Making Query')
        data = pd.read_sql(sql=query.query_retention_seller_pack(),
                           con=self.db.connection)
        self.logger.info('Query Ended')
        self.__data_retention_seller_packs_detail = data
        self.db.close_connection()

    def generate(self):
        self.data_retention_seller_packs_detail = self.config
        self.delete_from_retention_seller_packs_detail()
        self.save_retention_seller_packs_detail()
