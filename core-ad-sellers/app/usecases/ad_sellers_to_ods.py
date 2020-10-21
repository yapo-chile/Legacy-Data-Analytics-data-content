# pylint: disable=no-member
# pylint: disable=W0201
# utf-8
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams


class AdSellersToOds(Query):
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger

    # Query data from data warehouse
    @property
    def data_stg_sellers_created_daily(self):
        return self.__data_stg_sellers_created_daily

    @data_stg_sellers_created_daily.setter
    def data_stg_sellers_created_daily(self, config):
        db_source = Database(conf=config)
        output_df = db_source \
            .select_to_dict(self.get_stg_sellers_created_daily())
        db_source.close_connection()

        self.__data_stg_sellers_created_daily = output_df

    @property
    def data_stg_seller_pro_details(self):
        return self.__data_stg_seller_pro_details

    @data_stg_seller_pro_details.setter
    def data_stg_seller_pro_details(self, config):
        db_source = Database(conf=config)
        output_df = db_source \
            .select_to_dict(self.get_stg_sellers_created_daily())
        db_source.close_connection()

        self.__data_stg_seller_pro_details = output_df

    # Write data to data warehouse
    def save_to_ods_seller(self) -> None:
        db = Database(conf=self.config.dwh)
        db.execute_command(self.delete_ods_seller_table())
        db.insert_copy("ods", "seller", self.formatted_data)

    def update_ods_seller(self) -> None:
        db = Database(conf=self.config.dwh)
        db.execute_command(self.upd_ods_seller_table())
        db.close_connection()

    def save_to_ods_seller_pro_details(self) -> None:
        db = Database(conf=self.config.dwh)
        db.execute_command(self.delete_ods_seller_pro_details_table())
        db.insert_copy("ods", "seller_pro_details", self.formatted_data)

    def generate(self):
        # First step: insert into ods.seller
        self.logger.info('Starting ods.seller persistence')
        self.logger.info('Getting sellers data from DWH STG schema')
        self.data_stg_sellers_created_daily = self.config.dwh
        self.formatted_data = self.data_stg_sellers_created_daily
        self.logger.info('Executing ods.seller inserts')
        astypes = {"seller_id_pk": "Int64",
                   "seller_id_blocket_nk": "Int64",
                   "pri_pro_id_fk": "Int64"}
        self.formatted_data = self.formatted_data.astype(astypes)
        self.save_to_ods_seller()
        self.logger.info('Executed ods.seller persistence')

        # Second step: update ods.seller with stg.account data
        self.logger.info('Starting ods.seller updates')
        self.logger.info('Executing ods.seller updates')
        self.update_ods_seller()
        self.logger.info('Executed ods.seller updates')

        # Third step: insert into ods.seller_pro_details
        self.logger.info('Starting ods.seller_pro_details persistence')
        self.logger.info('Getting sellers pro data from DWH STG schema')
        self.data_stg_seller_pro_details = self.config.dwh
        self.formatted_data = self.data_stg_seller_pro_details
        self.logger.info('Executing ods.seller_pro_details inserts')
        astypes = {"seller_id_fk": "Int64", "account_id_nk": "Int64",
                   "user_id": "Int64", "category_rank": "Int64",
                   "category_id_fk": "Int64",
                   "category_main_id_fk": "Int64"}
        self.formatted_data = self.formatted_data.astype(astypes)
        self.save_to_ods_seller_pro_details()
        self.logger.info('Executed ods.seller_pro_details persistence')

        return True
