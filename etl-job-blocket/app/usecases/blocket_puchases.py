# pylint: disable=no-member
# utf-8
import sys
import logging
from infraestructure.athena import Athena
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams

class BlocketPuchases():
    def __init__(self,
                 config,
                 params: ReadParams) -> None:
        self.config = config
        self.params = params
        self.logger = logging.getLogger('BlocketPuchases')

    def save(self, data, table_name, configdb) -> None:
        query = Query(config, params)
        db = Database(conf=configdb)
        db.insert_data(table_name, data)
        db.close_connection()

    @property
    def stg_purchase_ios(self):
        return self.__stg_purchase_ios

    @stg_puchase_ios.setter
    def v(self, config):
        query = Query(config, self.params)
        db = Database(conf=config)
        data = db.select_to_dict(
            query.stg_purchase_ios())
        db.close_connection()
        self.__stg_purchase_ios = data

    @property
    def product_order_detail(self):
        return self.__product_order_detail

    @product_order_detail.setter
    def product_order_detail(self, config):
        query = Query(config, self.params)
        db = Database(conf=config)
        data = db.select_to_dict(
            query.product_order_detail())
        db.close_connection()
        self.__product_order_detail = data

    def generate(self):
        self.stg_purchase_ios = self.config.db
        self.save(self.stg_purchase_ios,
                  'stg.purchase_ios',
                  self.config.dw)
        self.product_order_detail = self.config.db
        self.save(self.product_order_detail,
                  'stg.product_order_detail',
                  self.config.dw)