# pylint: disable=no-member
# pylint: disable=W0201
# utf-8
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams


class AdSellersToStg(Query):
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger

    # Query data from data warehouse
    @property
    def data_blocket_users_account(self):
        return self.__data_blocket_users_account

    @data_blocket_users_account.setter
    def data_blocket_users_account(self, config):
        db_source = Database(conf=config)
        output_df = db_source \
            .select_to_dict(self.get_blocket_users_account())
        db_source.close_connection()
        if output_df.empty:
            raise Exception("users account etl got empty dataframe")

        self.__data_blocket_users_account = output_df

    @property
    def data_blocket_sellers_created_daily(self):
        return self.__data_blocket_sellers_created_daily

    @data_blocket_sellers_created_daily.setter
    def data_blocket_sellers_created_daily(self, config):
        db_source = Database(conf=config)
        output_df = db_source \
            .select_to_dict(self.get_blocket_sellers_created_daily())
        db_source.close_connection()
        if output_df.empty:
            raise Exception("sellers created daily etl got empty dataframe")

        self.__data_blocket_sellers_created_daily = output_df

    @property
    def data_blocket_account_params_is_pro(self):
        return self.__data_blocket_account_params_is_pro

    @data_blocket_account_params_is_pro.setter
    def data_blocket_account_params_is_pro(self, config):
        db_source = Database(conf=config)
        output_df = db_source \
            .select_to_dict(self.get_blocket_account_params_is_pro())
        db_source.close_connection()
        if output_df.empty:
            raise Exception("account params etl got empty dataframe")

        self.__data_blocket_account_params_is_pro = output_df

    # Write data to data warehouse
    def save_to_stg_account(self) -> None:
        db = Database(conf=self.config.dwh)
        db.execute_command(self.delete_stg_account_table())
        db.insert_copy("stg", "account", self.formatted_data)

    def save_to_stg_seller_created_daily(self) -> None:
        db = Database(conf=self.config.dwh)
        db.execute_command(self.delete_stg_seller_created_daily_table())
        db.insert_copy("stg", "seller_created_daily", self.formatted_data)

    def save_to_stg_seller_pro(self) -> None:
        db = Database(conf=self.config.dwh)
        db.execute_command(self.delete_stg_seller_pro_table())
        db.insert_copy("stg", "seller_pro", self.formatted_data)

    def generate(self):
        # First step: stg.account
        self.logger.info('Starting stg.account persistence')
        self.logger.info('Getting users account data from Blocket DB')
        self.data_blocket_users_account = self.config.db
        self.formatted_data = self.data_blocket_users_account
        self.logger.info('Executing stg.account inserts')
        astypes = {"user_id_nk": "Int64"}
        self.formatted_data = self.formatted_data.astype(astypes)
        self.save_to_stg_account()
        self.logger.info('Executed stg.account persistence')

        # Second step: stg.sellers_created_daily
        self.logger.info('Starting stg.sellers_created_daily persistence')
        self.logger.info('Getting users that are sellers \
            created daily data from Blocket DB')
        self.data_blocket_sellers_created_daily = self.config.db
        self.formatted_data = self.data_blocket_sellers_created_daily
        self.logger.info('Executing stg.sellers_created_daily inserts')
        astypes = {"seller_id_blocket_nk": "Int64",
                   "pri_pro_id_fk": "Int64"}
        self.formatted_data = self.formatted_data.astype(astypes)
        self.save_to_stg_seller_created_daily()
        self.logger.info('Executed stg.sellers_created_daily persistence')

        # Third step: stg.seller_pro
        self.logger.info('Starting stg.seller_pro persistence')
        self.logger.info('Getting categories which are pro user by \
            account_id data from Blocket DB')
        self.data_blocket_account_params_is_pro = self.config.db
        self.formatted_data = self.data_blocket_account_params_is_pro
        self.logger.info('Executing stg.seller_pro inserts')
        astypes = {"user_id": "Int64", "account_id": "Int64",
                   "category_rank": "Int64"}
        self.formatted_data = self.formatted_data.astype(astypes)
        self.save_to_stg_seller_pro()
        self.logger.info('Executed stg.seller_pro persistence')

        return True
