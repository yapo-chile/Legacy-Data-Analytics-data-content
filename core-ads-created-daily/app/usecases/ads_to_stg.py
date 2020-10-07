# pylint: disable=no-member
# utf-8
import datetime
from infraestructure.psql import Database
from utils.query import Query


class AdsToStg():

    # Query data from data warehouse
    @property
    def data_blocket_ads_created_daily(self):
        return self.__data_blocket_ads_created_daily

    @data_blocket_ads_created_daily.setter
    def data_blocket_ads_created_daily(self, config):
        query = Query(config, self.params)
        db_source = Database(conf=config)
        output_df = db_source.select_to_dict(query \
            .get_blocket_ads_created_daily())
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
        query = Query(config, self.params)
        db_source = Database(conf=config)
        output_df = db_source.select_to_dict(query \
            .get_stg_ads_approved_daily())
        db_source.close_connection()

        self.__data_dwh_ads_approved_daily = output_df

    # Query data from data warehouse
    @property
    def data_dwh_ads_deleted_daily(self):
        return self.__data_dwh_ads_deleted_daily

    @data_dwh_ads_deleted_daily.setter
    def data_dwh_ads_deleted_daily(self, config):
        query = Query(config, self.params)
        db_source = Database(conf=config)
        output_df = db_source.select_to_dict(query \
            .get_stg_ads_deleted_daily())
        db_source.close_connection()

        output_df['reason_removed_id_fk'] = \
            output_df['reason_removed_id_fk'].fillna(0).astype(int)

        output_df['reason_removed_detail_id_fk'] = \
            output_df['reason_removed_detail_id_fk'].fillna(0).astype(int)

        self.__data_dwh_ads_deleted_daily = output_df

    # Write data to data warehouse
    def save_to_stg_ad(self) -> None:
        query = Query(self.config, self.params)
        db = Database(conf=self.config.dwh)
        db.execute_command(query.delete_stg_ad_table())
        #self.data_blocket_ads_created_daily = self.config.db
        self.logger.info('Executing stg.ad inserts cycle')
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
            db.insert_data(query.insert_to_stg_ad_table(), data_row)
        self.logger.info('INSERT dm_analysis.temp_stg_ad COMMIT.')
        self.logger.info('Executed data persistence cycle')
        db.close_connection()

    # Write data to data warehouse
    def save_to_stg_ad_approved(self) -> None:
        query = Query(self.config, self.params)
        db = Database(conf=self.config.dwh)
        db.execute_command(query.delete_stg_ad_approved_table())
        self.data_dwh_ads_approved_daily = self.config.dwh
        self.logger.info('Executing stg.ad_approved inserts cycle')
        for row in self.data_dwh_ads_approved_daily.itertuples():
            data_row = [(row.ad_id_nk, row.approval_date,
                         row.price, row.list_id_nk)]
            db.insert_data(query.insert_to_stg_ad_approved_table(), data_row)
        self.logger.info('INSERT dm_analysis.temp_stg_ad_approved COMMIT.')
        self.logger.info('Executed data persistence cycle')
        db.close_connection()

    # Write data to data warehouse
    def save_to_stg_ad_deleted(self) -> None:
        query = Query(self.config, self.params)
        db = Database(conf=self.config.dwh)
        db.execute_command(query.delete_stg_ad_deleted_table())
        self.data_dwh_ads_deleted_daily = self.config.dwh
        self.logger.info('Executing stg.ad_deleted inserts cycle')
        for row in self.data_dwh_ads_deleted_daily.itertuples():
            data_row = [(row.ad_id_nk, row.deletion_date,
                         row.reason_removed_id_fk,
                         row.reason_removed_detail_id_fk)]
            db.insert_data(query.insert_to_stg_ad_deleted_table(), data_row)
        self.logger.info('INSERT dm_analysis.temp_stg_ad_deleted COMMIT.')
        self.logger.info('Executed data persistence cycle')
        db.close_connection()
