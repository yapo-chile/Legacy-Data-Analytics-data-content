# pylint: disable=no-member
# utf-8
import sys
import logging
from infraestructure.athena import Athena
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams

class BlocketPacks():
    def __init__(self, config, params: ReadParams) -> None:
        self.config = config
        self.params = params
        self.logger = logging.getLogger('BlocketPacks')

    def save(self, data, table_name, configdb) -> None:
        query = Query(config, params)
        db = Database(conf=configdb)
        db.insert_data(table_name, data)
        db.close_connection()

    # Query data from data warehouse
    @property
    def stg_pack_autos(self):
        return self.__stg_pack_autos

    @stg_pack_autos.setter
    def stg_pack_autos(self, config):
        query = Query(config, self.params)
        db_source = Database(conf=config)
        data_dwh = db_source.select_to_dict(
            query.stg_pack_autos())
        db_source.close_connection()
        self.__stg_pack_autos = data_dwh

    @property
    def stg_pack_autos(self):
        return self.__pack_manual_acepted

    @pack_manual_acepted.setter
    def pack_manual_acepted(self, config):
        query = Query(config, self.params)
        db_source = Database(conf=config)
        data_dwh = db_source.select_to_dict(
            query.pack_manual_acepted())
        db_source.close_connection()
        self.__pack_manual_acepted = data_dwh

    @property
    def ads_disabled_pack_autos(self):
        return self.__ads_disabled_pack_autos

    @ads_disabled_pack_autos.setter
    def ads_disabled_pack_autos(self, config):
        query = Query(config, self.params)
        db_source = Database(conf=config)
        data_dwh = db_source.select_to_dict(
            query.ads_disabled_pack_autos())
        db_source.close_connection()
        self.__ads_disabled_pack_autos = data_dwh

    @property
    def packs(self):
        return self.__packs

    @packs.setter
    def packs(self, config):
        query = Query(config, self.params)
        db_source = Database(conf=config)
        data_dwh = db_source.select_to_dict(
            query.packs())
        db_source.close_connection()
        self.__packs = data_dwh

    def generate(self):
        self.stg_pack_autos = self.config.db
        self.save(self.stg_pack_autos,
                  'stg.temp_pack',
                  self.config.dw)

        self.pack_manual_acepted = self.config.db
        self.save(self.pack_manual_acepted,
                  'stg.pack_manual_accepted',
                  self.config.dw)

        self.ads_disabled_pack_autos = self.config.db
        self.save(self.ads_disabled_pack_autos,
                  'stg.ads_disabled_pack_autos',
                  self.config.dw)

        self.packs = self.config.db
        self.save(self.packs,
                  'stg.packs',
                  self.config.dw)
