# pylint: disable=no-member
# utf-8
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams


class AdsToOds(Query):
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger

    # Query data from data warehouse
    @property
    def data_stg_ads_created_daily(self):
        return self.__data_stg_ads_created_daily

    @data_stg_ads_created_daily.setter
    def data_stg_ads_created_daily(self, config):
        db_source = Database(conf=config)
        output_df = db_source \
            .select_to_dict(self.get_stg_ads_created_daily())
        db_source.close_connection()

        self.__data_stg_ads_created_daily = output_df

    # Write data to data warehouse
    def clean_ods_ad(self) -> None:
        db = Database(conf=self.config.dwh)
        db.execute_command(self.delete_ods_ad_table())

    def save_to_ods_ad(self) -> None:
        db = Database(conf=self.config.dwh)
        db.insert_copy(self.formatted_data, "ods", "ad")

    def update_ods_ad(self) -> None:
        db = Database(conf=self.config.dwh)
        db.execute_command(self.upd_approval_date_to_ods_ad_table())
        db.execute_command(self.upd_left_approval_date_to_ods_ad_table())
        db.execute_command(self.upd_deletion_date_to_ods_ad_table())
        db.execute_command(self.upd_rank_approval_to_ods_ad_table())
        db.execute_command(self.upd_first_approval_to_ods_seller_table())
        db.close_connection()

    # Executer method
    def generate(self):
        # First step: inserts into ods.ad
        self.logger.info('Starting ods.ad persistence')
        self.logger.info('Getting ads created daily data from DWH DB')
        self.clean_ods_ad()
        self.data_stg_ads_created_daily = self.config.dwh
        self.formatted_data = self.data_stg_ads_created_daily
        self.logger.info('Executing ods.ad inserts')
        astypes = {"ad_type_id_fk": "Int64", "category_id_fk": "Int64",
                   "platform_id_fk": "Int64", "pri_pro_id_fk": "Int64",
                   "region_id_fk": "Int64", "seller_id_fk": "Int64",
                   "ad_id_nk": "Int64", "price": "Int64"}
        self.formatted_data = self.formatted_data.astype(astypes)
        self.save_to_ods_ad()
        self.logger.info('Executed ods.ad persistence')

        # Second step: update ods.ad with approval and deletion date data
        self.logger.info('Starting ods.ad updates')
        self.logger.info('Executing ods.ad updates')
        self.update_ods_ad()
        self.logger.info('Executed ods.ad updates')

        return True
