# pylint: disable=no-member
# utf-8
import logging
import datetime
from infraestructure.psql import Database
from utils.query import Query


class AdsToOds():

    # Query data from data warehouse
    @property
    def data_stg_ads_created_daily(self):
        return self.__data_stg_ads_created_daily

    @data_stg_ads_created_daily.setter
    def data_stg_ads_created_daily(self, config):
        query = Query(config, self.params)
        db_source = Database(conf=config)
        output_df = db_source.select_to_dict(query \
            .get_stg_ads_created_daily())
        db_source.close_connection()

        self.__data_stg_ads_created_daily = output_df

    # Write data to data warehouse
    def save_to_ods_ad(self) -> None:
        query = Query(self.config, self.params)
        db = Database(conf=self.config.dwh)
        db.execute_command(query.delete_ods_ad_table())
        self.data_stg_ads_created_daily = self.config.dwh

        ## Prueba con 10 rows "quitar el print"
        print("Otuput_stg: ", self.data_stg_ads_created_daily.head(10))

        logging.info('Executing ods.ad inserts cycle')
        ## Prueba con 100 rows "quitar el head(100)"
        for row in self.data_stg_ads_created_daily.head(100).itertuples():
            data_row = [(row.ad_type_id_fk, row.category_id_fk, row.platform_id_fk,
                         row.pri_pro_id_fk, row.region_id_fk, row.seller_id_fk,
                         row.creation_date, row.ad_id_nk, row.insert_date,
                         row.update_date, row.action_type, row.price,
                         row.communes_id_nk, row.phone, row.body, row.subject,
                         row.user_name)]
            db.insert_data(query.insert_ad_created_to_ods_ad_table(), data_row)
        logging.info('INSERT dm_analysis.temp_ods_ad COMMIT.')
        logging.info('Executed data persistence cycle')

        logging.info('Executing ods.ad updates cycle')
        db.execute_command(query.upd_approval_date_stg_to_ods_ad_table())
        logging.info('UPDATE dm_analysis.temp_ods_ad COMMIT.')
        db.execute_command(query.upd_approval_date_ods_ad_table())
        logging.info('UPDATE dm_analysis.temp_ods_ad COMMIT.')
        db.execute_command(query.upd_deletion_date_stg_to_ods_ad_table())
        logging.info('UPDATE dm_analysis.temp_ods_ad COMMIT.')
        logging.info('Executed data persistence cycle')
        db.close_connection()
