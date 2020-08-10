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

    def delete_from_retention_seller_packs_detail(self):
        """
        Method that delete data from month in process if exist
        """
        query = QueryRSPDetail(self.config, self.params)
        db = Database(conf=self.config.db)
        self.logger.info('Executing query to delete data')
        db.execute_command(
            query.delete_retention_sellers_packs_detail(self.month_id))
        self.logger.info('Query executed')
        db.close_connection()

    def save_retention_seller_packs_detail(self) -> None:
        """
        Method that save data in DWH in table
        retention seller packs detail
        """
        query = QueryRSPDetail(self.config, self.params)
        db = Database(conf=self.config.db)
        db.insert_data(
            table_name=query.table_dest_rsp_detail,
            data=self.data_retention_seller_packs_detail)
        db.close_connection()

    @property
    def data_retention_seller_packs_detail(self):
        return self.__data_retention_seller_packs_detail

    @data_retention_seller_packs_detail.setter
    def data_retention_seller_packs_detail(self, config):
        query = QueryRSPDetail(config, self.params)
        db = Database(conf=config.db)
        self.logger.info('Executing query to get data from dwh')
        data = pd.read_sql(sql=query.query_retention_seller_packs_detail(),
                           con=db.connection)
        self.logger.info('Query executed')
        db.close_connection()
        self.__data_retention_seller_packs_detail = data

    def generate(self):
        self.data_retention_seller_packs_detail = self.config
        self.delete_from_retention_seller_packs_detail()
        self.save_retention_seller_packs_detail()
