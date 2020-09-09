# pylint: disable=no-member
# utf-8
import logging
from infraestructure.psql import Database
from utils.query import Query


class OdsBlocket():
    def __init__(self, config, params) -> None:
        self.config = config
        self.params = params
        self.logger = logging.getLogger('BlocketPacks')
        self.select_purchase_ios = [
            'product_order_nk',
            'creation_date',
            'payment_date',
            'price',
            'status',
            'insert_date',
            'product_id_nk',
            'ad_id_nk',
            'user_id_nk',
            'payment_platform',
            'price_clp'
        ]

    def delete_packs(self) -> None:
        query = Query(self.config, self.params)
        db = Database(conf=self.config.dw)
        self.logger.info('Iniciando query: Delete de Packs')
        db.execute_command(query.delete_ods_packs())
        db.close_connection()

    def save(self, data, schema, table_name, configdb) -> None:
        db = Database(conf=configdb)
        self.logger.info('Iniciando inserción de datos')
        db.insert_copy(schema, table_name, data)
        self.logger.info(
            'Datos insertados en {}.{}'.format(schema, table_name))
        db.close_connection()

    @property
    def stg_packs(self):
        return self.__stg_packs

    @stg_packs.setter
    def stg_packs(self, config):
        query = Query(config, self.params)
        db = Database(conf=config)
        data = db.select_to_dict(
            query.stg_packs())
        data = data.astype(
            {
                'account_id': 'Int64',
                'days': 'Int64',
                'slots': 'Int64',
                'product_id': 'Int64',
                'seller_id_fk': 'Int64'
            }
        )
        
        self.__stg_packs = data
        db.close_connection()

    @property
    def dw_str_purchase_ios(self):
        return self.__dw_str_purchase_ios

    @dw_str_purchase_ios.setter
    def dw_str_purchase_ios(self, config):
        query = Query(config, self.params)
        db = Database(conf=config)
        data = db.select_to_dict(
            query.dw_str_purchase_ios())
        db.close_connection()
        self.__dw_str_purchase_ios = data

    @property
    def ods_product_order_detail(self):
        return self.__ods_product_order_detail

    @ods_product_order_detail.setter
    def ods_product_order_detail(self, config):
        query = Query(config, self.params)
        db = Database(conf=config)
        data = db.select_to_dict(
            query.ods_product_order_detail())
        db.close_connection()
        self.__ods_product_order_detail = data

    def generate(self):
        self.delete_packs()
        self.stg_packs = self.config.dw
        self.save(self.stg_packs,
                  'ods',
                  'packs',
                  self.config.dw)
        self.dw_str_purchase_ios = self.config.dw
        self.save(
            self.dw_str_purchase_ios[
                self.select_purchase_ios],
            'ods',
            'product_order_ios',
            self.config.dw
        )
        self.ods_product_order_detail = self.config.dw
        self.save(
            self.ods_product_order_detail,
            'ods',
            'product_order_detail',
            self.config.dw
        )
