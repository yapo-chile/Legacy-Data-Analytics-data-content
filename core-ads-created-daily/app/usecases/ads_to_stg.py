# pylint: disable=no-member
# utf-8
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
        if output_df.empty:
            raise Exception("ads created daily etl got empty dataframe")

        self.__data_blocket_ads_created_daily = output_df

    # Write data to data warehouse
    def save_to_stg_ad(self) -> None:
        db = Database(conf=self.config.dwh)
        db.execute_command(self.delete_stg_ad_table())
        db.insert_copy(self.formatted_data, "stg", "ad")

    # Executer method
    def generate(self):
        # First step: stg.ad
        self.logger.info('Starting stg.ad persistence')
        self.logger.info('Getting ads created daily data from Blocket DB')
        self.data_blocket_ads_created_daily = self.config.db
        self.formatted_data = self.data_blocket_ads_created_daily
        self.logger.info('Executing stg.ad inserts')
        astypes = {"ad_id": "Int64", "list_id": "Int64",
                   "user_id": "Int64", "account_id": "Int64",
                   "category": "Int64", "region": "Int64",
                   "price": "Int64"}
        self.formatted_data = self.formatted_data.astype(astypes)
        self.save_to_stg_ad()
        self.logger.info('Executed stg.ad persistence')

        return True
