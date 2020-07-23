from datetime import timedelta
import logging
import pandas as pd
from utils.query import AdsQuery
from utils.read_params import ReadParams
from infraestructure.psql import Database
from infraestructure.aws import AwsCredentials

class Liquidity():
    """
    Class that get ads from yapo dwh and save in s3 bucket
    """
    def __init__(self, config, params: ReadParams) -> None:
        """ """
        self.config = config
        self.params = params
        self.logger = logging.getLogger('Liquidity-proc')

    @property
    def ads_data(self) -> str:
        """
        Get from ads_data method
        """
        return self.__ads_data

    @ads_data.setter
    def ads_data(self, config):
        """
        Setter from ads_data method
        """
        query = AdsQuery(date=self.day_).get_ads()
        if self.params.date_from.strftime("%Y-%m-%d") == self.day_:
            self.db_con = Database(config)
        self.logger.info('Starting database query')
        self.__ads_data = pd.read_sql(sql=query, con=self.db_con.connection)
        self.logger.info('Database query finished')
        if self.params.date_to.strftime("%Y-%m-%d") == self.day_:
            self.db_con.close_connection()

    @property
    def day_(self):
        """
        Get from day_ private variable
        """
        return self.__day_

    @day_.setter
    def day_(self, day):
        """
        Set from day_ private variable
        """
        self.__day_ = day

    def generate_file_to_day(self):
        """
        Function that makes a query to database and save data in a
        file in aws s3 bucket indicated in aws secret
        """
        year_process = self.day_[:4]
        month_process = self.day_[5:7]
        day_process = self.day_[8:]
        s3_bucket = "yapo-s3-dev-data"
        bucket_prefix = "/dev/insights/liquidity/yapo/content/"
        aws_prefix = "s3://{bucket}{prefix}"\
            .format(bucket=s3_bucket, prefix=bucket_prefix)
        path_partitions = "year={year}/month={month}/day={day}/"\
            .format(year=year_process, month=month_process, day=day_process)
        name_parquet = "ads.parquet"
        s3_url = aws_prefix+path_partitions+name_parquet
        self.ads_data = self.config.db
        try:
            self.ads_data.to_parquet(s3_url, compression='snappy')
            self.logger.info('File saved in {path}'.format(path=s3_url))
        except ConnectionError as error_connect:
            self.logger.info('''
            Error (E=1) uploading file s3 bucket for day {day}, error: {error}
            '''.format(day=self.day_, error=error_connect))

    def generate_for_time_frame(self):
        """
        Method that iterate for day in a time frame defined in execution params
        """
        AwsCredentials(self.config.aws)
        date_var_ = self.params.date_from
        n_days = range((self.params.date_to - self.params.date_from).days +1)
        for _ in n_days:
            self.day_ = date_var_.strftime("%Y-%m-%d")
            self.logger.info('Starting process for date: {date}'\
                .format(date=self.day_))
            self.generate_file_to_day()
            date_var_ = date_var_ + timedelta(days=1)
