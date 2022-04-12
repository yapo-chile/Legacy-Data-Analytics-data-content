# pylint: disable=no-member
# utf-8
import logging
from infraestructure.psql import Database
from utils.query import Queries
from utils.read_params import ReadParams


class AdsByUser(Queries):
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger
        self.stat_schema = "public"
        self.stat_table = "ads_by_user"
        self.dwh_schema = "ods"
        self.dwh_table = "ads_by_user"

    @property
    def ads(self):
        return self.__ads
    
    @ads.setter
    def ads(self, conf):
        dwh = Database(conf=conf)
        data = dwh.select_to_dict(self.get_data())
        data['date_time'] = data['date_time'].apply(lambda x: x.strftime('%Y-%m-%d'))
        for column in ["user_id", "category", "nof_ads", "nof_sold_ads"]:
            data[column] = data[column].astype(int)
        self.__ads = data

    def clean_statistics(self):
        dwh = Database(conf=self.config.stat_database)
        dwh.execute_command(self.clean_statistics_table())

    def clean_dwh(self):
        dwh = Database(conf=self.config.database)
        dwh.execute_command(self.clean_dwh_table())

    def insert_to_table(self, squema, table, conf):
        dwh = Database(conf=conf)
        dwh.insert_copy(self.ads, squema, table)

    def generate(self):
        self.ads = self.config.database
        self.logger.info("data sample is: {}".format(self.ads.head()))
        
        # cleaning data for datawarehouse and inserting it to table
        self.clean_dwh()
        self.insert_to_table(self.dwh_schema, self.dwh_table, self.config.database)

        # cleaning data for statistics and inserting it to table
        self.clean_statistics()
        self.insert_to_table(self.stat_schema, self.stat_table, self.config.stat_database)

        return True
