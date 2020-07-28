# pylint: disable=no-member
# utf-8
import logging
import datetime
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams


class Process():
    def __init__(self,
                 config,
                 params: ReadParams) -> None:
        self.config = config
        self.params = params

    # Query data from data warehouse
    @property
    def data_ads_created_daily(self):
        return self.__data_ads_created_daily

    @data_ads_created_daily.setter
    def data_ads_created_daily(self, config):
        query = Query(config, self.params)
        db_source = Database(conf=config)
        output_df = db_source.select_to_dict(query \
            .get_data_ads_created_daily())
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

        self.__data_ads_created_daily = output_df

    # Write data to data warehouse
    def save(self) -> None:
        query = Query(self.config, self.params)
        db = Database(conf=self.config.dwh)
        # db.execute_command(query.delete_output_dw_table())
        #i = 0
        logging.info('Inicia ciclo de persistencia de los datos')
        for row in self.data_ads_created_daily.itertuples():
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
            #print('data_row_' + str(i) + ' :', data_row)
            db.insert_data(query.insert_output_to_dw(), data_row)
            #i = i + 1
        logging.info('INSERT dm_analysis.temp_stg_ads COMMIT.')
        db.close_connection()

    def generate(self):
        self.data_ads_created_daily = self.config.db
        #print("Output: ", self.data_ads_created_daily)
        self.save()
