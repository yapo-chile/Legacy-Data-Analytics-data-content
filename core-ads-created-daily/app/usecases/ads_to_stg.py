# pylint: disable=no-member
# utf-8
import datetime
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams


class AdsToStg(Query):
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger

    # Query data from data warehouse
    @property
    def data_blocket_ads_created_daily(self):
        return self.__data_blocket_ads_created_daily

    @data_blocket_ads_created_daily.setter
    def data_blocket_ads_created_daily(self, config):
        db_source = Database(conf=config)
        output_df = db_source \
            .select_to_dict(self.get_blocket_ads_created_daily())
        db_source.close_connection()

        output_df['list_id'] = output_df['list_id'].fillna(0)\
        .astype(int)

        output_df['account_id'] = output_df['account_id'].fillna(0)\
        .astype(int)

        output_df['price'] = output_df['price'].fillna(0)\
        .astype(int)

        output_df["creation_date"] = output_df["creation_date"]. \
        fillna(datetime.datetime(2199, 12, 31, 0, 0, 0)).astype(str)

        output_df["approval_date"] = output_df["approval_date"]. \
        fillna(datetime.datetime(2199, 12, 31, 0, 0, 0)).astype(str)

        output_df["deletion_date"] = output_df["deletion_date"]. \
        fillna(datetime.datetime(2199, 12, 31, 0, 0, 0)).astype(str)

        self.__data_blocket_ads_created_daily = output_df

    # Query data from data warehouse
    @property
    def data_dwh_ads_approved_daily(self):
        return self.__data_dwh_ads_approved_daily

    @data_dwh_ads_approved_daily.setter
    def data_dwh_ads_approved_daily(self, config):
        db_source = Database(conf=config)
        output_df = db_source \
            .select_to_dict(self.get_stg_ads_approved_daily())
        db_source.close_connection()

        self.__data_dwh_ads_approved_daily = output_df

    # Query data from data warehouse
    @property
    def data_dwh_ads_deleted_daily(self):
        return self.__data_dwh_ads_deleted_daily

    @data_dwh_ads_deleted_daily.setter
    def data_dwh_ads_deleted_daily(self, config):
        db_source = Database(conf=config)
        output_df = db_source \
            .select_to_dict(self.get_stg_ads_deleted_daily())
        db_source.close_connection()

        output_df['reason_removed_id_fk'] = \
            output_df['reason_removed_id_fk'].fillna(0).astype(int)

        output_df['reason_removed_detail_id_fk'] = \
            output_df['reason_removed_detail_id_fk'].fillna(0).astype(int)

        self.__data_dwh_ads_deleted_daily = output_df

    # Write data to data warehouse
    def save_to_stg_ad(self) -> None:
        db = Database(conf=self.config.dwh)
        db.execute_command(self.delete_stg_ad_table())
        for row in self.data_blocket_ads_created_daily.itertuples():
            data_row = [(row.ad_id, row.list_id, row.user_id,
                         row.account_id, row.email, row.platform_id_nk,
                         row.creation_date, row.approval_date,
                         row.deletion_date, row.category, row.region,
                         row.type, row.company_ad, row.price,
                         row.reason_removed_id_nk,
                         row.reason_removed_detail_id_nk,
                         row.action_type, row.communes_id_nk,
                         row.phone, row.body, row.subject,
                         row.user_name)]
            db.insert_data(self.insert_to_stg_ad_table(), data_row)
        db.close_connection()

    # Write data to data warehouse
    def save_to_stg_ad_approved(self) -> None:
        db = Database(conf=self.config.dwh)
        db.execute_command(self.delete_stg_ad_approved_table())
        for row in self.data_dwh_ads_approved_daily.itertuples():
            data_row = [(row.ad_id_nk, row.approval_date,
                         row.price, row.list_id_nk)]
            db.insert_data(self.insert_to_stg_ad_approved_table(), data_row)
        db.close_connection()

    # Write data to data warehouse
    def save_to_stg_ad_deleted(self) -> None:
        #query = Query(self.config, self.params)
        db = Database(conf=self.config.dwh)
        db.execute_command(self.delete_stg_ad_deleted_table())
        for row in self.data_dwh_ads_deleted_daily.itertuples():
            data_row = [(row.ad_id_nk, row.deletion_date,
                         row.reason_removed_id_fk,
                         row.reason_removed_detail_id_fk)]
            db.insert_data(self.insert_to_stg_ad_deleted_table(), data_row)
        db.close_connection()

    def generate(self):
        # First step: stg.ad
        self.logger.info('Starting stg.ad persistence')
        self.logger.info('Getting ads created daily data from Blocket DB')
        self.data_blocket_ads_created_daily = self.config.db
        self.logger.info('Executing stg.ad inserts')
        self.save_to_stg_ad()
        self.logger.info('Executed stg.ad persistence')

        # Second step: stg.ad_approved
        self.logger.info('Starting stg.ad_approved persistence')
        self.logger.info('Getting ads approved date data from DWH DB')
        self.data_dwh_ads_approved_daily = self.config.dwh
        self.logger.info('Executing stg.ad_approved inserts')
        self.save_to_stg_ad_approved()
        self.logger.info('Executed stg.ad_approved persistence')

        # Third step: stg.ad_deleted
        self.logger.info('Starting stg.ad_deleted persistence')
        self.logger.info('Getting ads deletion date data from DWH DB')
        self.data_dwh_ads_deleted_daily = self.config.dwh
        self.logger.info('Executing stg.ad_deleted inserts')
        self.save_to_stg_ad_deleted()
        self.logger.info('Executed stg.ad_deleted persistence')

        return True
