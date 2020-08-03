# pylint: disable=no-member
# utf-8
import logging
import pandas as pd
from infraestructure.psql import Database
from utils.query_rsp_detail import QueryRSPDetail
from utils.read_params import ReadParams

class RetentionSellerPacksDetail():

    def __init__(self, config, params: ReadParams) -> None:
        self.config = config
        self.params = params
        self.logger = logging.getLogger('Retention-seller-pack-detail')
        self.month_id = self.params.date_from.strftime('%Y%m')
        self.db = None

    def delete_from_retention_seller_packs_detail(self):
        """
        Method that delete data from month in process if exist
        """
        query = QueryRSPDetail(self.config, self.params)
        if self.db is None:
            self.db = Database(conf=self.config.db)
        self.logger.info('Executing query delete')
        self.db.execute_command(
            query.delete_retention_sellers_packs_detail(self.month_id))
        self.logger.info('Query executed')

    def save_retention_seller_packs_detail(self) -> None:
        """
        Method that save data in DWH in table
        retention seller packs detail
        """
        query = QueryRSPDetail(self.config, self.params)
        if self.db is None:
            self.db = Database(conf=self.config.db)
        n_new_regs = len(self.data_retention_seller_packs_detail.index)
        self.logger.info('Inserting new data, {num_regs} new registers'\
            .format(num_regs=n_new_regs))
        self.db.insert_data(
            table_name=query.table_dest_rsp_detail,
            data=self.data_retention_seller_packs_detail)
        self.db.close_connection()

    @property
    def data_retention_seller_packs_detail(self):
        return self.__data_retention_seller_packs_detail

    @data_retention_seller_packs_detail.setter
    def data_retention_seller_packs_detail(self, config):
        query = QueryRSPDetail(config, self.params)
        if self.db is None:
            self.db = Database(conf=config.db)
        self.logger.info('Making Query')
        data = pd.read_sql(sql=query.query_retention_seller_packs_detail(),
                           con=self.db.connection)
        self.logger.info('Query Ended')
        self.__data_retention_seller_packs_detail = data

    def generate(self):
        self.data_retention_seller_packs_detail = self.config
        self.delete_from_retention_seller_packs_detail()
        self.save_retention_seller_packs_detail()
