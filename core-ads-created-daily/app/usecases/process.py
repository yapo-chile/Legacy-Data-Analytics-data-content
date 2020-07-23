# pylint: disable=no-member
# utf-8
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
        self.__data_ads_created_daily = output_df

    def generate(self):
        self.data_ads_created_daily = self.config.db
