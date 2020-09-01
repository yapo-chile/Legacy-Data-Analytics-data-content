# pylint: disable=no-member
# utf-8
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.encoders import encode_base64
import pandas as pd
import numpy as np
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams

class SendEmailBesedo():
    def __init__(self, config, params: ReadParams) -> None:
        self.config = config
        self.params = params
        self.time_format = "%Y-%m-%d %H:%M:%S"
        self.file_name = 'file_besedo.xls'
        self.dict_columns = {'< 5 min':'avisos - < 5 min',
                             '(5:00 - 14:59)min':'avisos - 5:00 - 14:59min',
                             '(15:00 - 29:59)min':'avisos - 15:00 - 29:59min',
                             '(30:00 - 44:59)min':'avisos - 30:00 - 44:59min',
                             '(45:00 - 59:59)min':'avisos - 45:00 - 59:59min',
                             '(1:00 - 1:30)hrs':'avisos - 1:00 - 1:30hrs',
                             '(1:31 - 2:00)hrs':'avisos - 1:31 - 2:00hrs',
                             '(2:01 - 3:00)hrs':'avisos - 2:01 - 3:00hrs',
                             '(3:01 - 4:00)hrs':'avisos - 3:01 - 4:00hrs',
                             '> 4 hrs':'avisos -> 4 hrs'}
        self.columns_final = [
            "review_time", "queue", "< 5 min",
            "(5:00 - 14:59)min", "(15:00 - 29:59)min",
            "(30:00 - 44:59)min", "(45:00 - 59:59)min",
            "(1:00 - 1:30)hrs", "(1:31 - 2:00)hrs",
            "(2:01 - 3:00)hrs", "(3:01 - 4:00)hrs", "> 4 hrs",
            "avisos - < 5 min", "avisos - 5:00 - 14:59min",
            "avisos - 15:00 - 29:59min", "avisos - 30:00 - 44:59min",
            "avisos - 45:00 - 59:59min", "avisos - 1:00 - 1:30hrs",
            "avisos - 1:31 - 2:00hrs", "avisos - 2:01 - 3:00hrs",
            "avisos - 3:01 - 4:00hrs", "avisos -> 4 hrs"]
        self.columns_to_tipo = [
            'action_type', 'action_type_2', 'action_type_3',
            'admin_id', 'action', 'time_stamp_exit',
            'time_stamp_creation', 'time_stamp_creation_lag']
        self.logger = logging.getLogger('send-email-besedo')

    @property
    def data_review_ads_base(self):
        return self.__data_review_ads_base

    @data_review_ads_base.setter
    def data_review_ads_base(self, config):
        query = Query(config, self.params)
        db_source = Database(conf=config)
        data_review_ads_base = db_source.select_to_dict(
            query.query_get_data_reviews())
        db_source.close_connection()
        self.__data_review_ads_base = data_review_ads_base

    def get_minutes_of_difference(self, date1, date2):
        diff = (date1-date2)
        minutes_of_days = diff.days*24*60
        minutes_of_seconds = diff.seconds//60
        return minutes_of_days+minutes_of_seconds

    def conditions_type_action(self, row):
        action_type = ''
        if row[6] == row[7]:
            action_type = 'calidad'
        elif row[0] in ('adminedit', 'post_refusal') or\
             row[1] in ('adminedit', 'post_refusal'):
            action_type = 'calidad'
        elif row[1] == 'disable' and row[4] == 'refused' and\
             row[0] == 'status_change':
            action_type = 'calidad'
        elif row[1] == 'remove_gallery' and\
             row[5] is pd.NaT and row[3] == 141:
            action_type = 'calidad'
        elif row[7] is pd.NaT and row[1] is np.nan and\
             row[2] == 'post_refusal':
            action_type = 'calidad'
        elif row[0] == 'bump' and row[1] == 'bump' and\
             row[2] == 'bump' and row[4] == 'refused':
            action_type = 'calidad'
        else:
            action_type = 'revision'
        return action_type

    def conditions_range_time_revision(self, time_creation_exit_mins):
        # pylint: disable=R0912
        range_time_revision = None
        if time_creation_exit_mins < 5:
            range_time_revision = '< 5 min'
        elif time_creation_exit_mins < 15:
            range_time_revision = '(5:00 - 14:59)min'
        elif time_creation_exit_mins < 30:
            range_time_revision = '(15:00 - 29:59)min'
        elif time_creation_exit_mins < 45:
            range_time_revision = '(30:00 - 44:59)min'
        elif time_creation_exit_mins < 60:
            range_time_revision = '(45:00 - 59:59)min'
        elif time_creation_exit_mins <= 90:
            range_time_revision = '(1:00 - 1:30)hrs'
        elif time_creation_exit_mins <= 120:
            range_time_revision = '(1:31 - 2:00)hrs'
        elif time_creation_exit_mins <= 180:
            range_time_revision = '(2:01 - 3:00)hrs'
        elif time_creation_exit_mins <= 240:
            range_time_revision = '(3:01 - 4:00)hrs'
        else:
            range_time_revision = '> 4 hrs'
        return range_time_revision

    def complete_rows(self, df):
        list_columns = ['review_time', 'queue', 'total_ads',
                        '< 5 min', '(5:00 - 14:59)min', '(15:00 - 29:59)min',
                        '(30:00 - 44:59)min', '(45:00 - 59:59)min',
                        '(1:00 - 1:30)hrs', '(1:31 - 2:00)hrs',
                        '(2:01 - 3:00)hrs', '(3:01 - 4:00)hrs',
                        '> 4 hrs']
        column_diff = list(set(list_columns) - set(df.reset_index().columns))
        if column_diff != []:
            for column in column_diff:
                df[column] = None
        return df.fillna(0)

    def getting_reviews_time_per_queue(self, df):
        df = df.sort_values(
            by=['ad_id', 'action_id', 'time_stamp_creation'],
            ascending=True)
        df['time_stamp_creation_lag'] = (df.groupby(['ad_id', 'action_id'])\
                                        ['time_stamp_creation'].shift(1)[:-1])
        select = ['ad_id', 'action_type', 'queue', 'review_time',
                  'review_time_date', 'admin_id', 'category', 'action',
                  'action_id', 'time_stamp_exit', 'action_type_3',
                  'admin_fullname', 'grupo_revision', 'time_stamp_creation',
                  'action_type_2', 'time_stamp_creation_lag']
        df = df[select][df['grupo_revision'] == 'Besedo']
        df['time_stamp_exit'] = pd.to_datetime(df['time_stamp_exit'],
                                               format=self.time_format)
        df['time_stamp_creation'] = pd.to_datetime(df['time_stamp_creation'],
                                                   format=self.time_format)
        df['review_time'] = pd.to_datetime(df['review_time'],
                                           format=self.time_format)
        df['tipo_accion'] = [self.conditions_type_action(row)
                             for row in df[self.columns_to_tipo].to_numpy()]
        df['real_action_type'] = df.apply(
            lambda x: x['action_type'] if x['action_type_2'] is None
            else x['action_type_2'],
            axis=1)
        df['time_stamp_exit_real'] = df.apply(
            lambda x: x['review_time'] if x['time_stamp_exit'] is pd.NaT
            else x['time_stamp_exit'],
            axis=1)
        df = df[df.tipo_accion == "revision"].reset_index(drop=True)
        df['tpo_creation_exit_min_real'] = df.apply(
            lambda x: self.get_minutes_of_difference(x['time_stamp_exit_real'],
                                                     x['time_stamp_creation']),
            axis=1)
        df['rango_tiempo_revision'] = df['tpo_creation_exit_min_real'].apply(
            self.conditions_range_time_revision)
        df = df.drop('review_time', axis=1)\
                .rename(columns={'review_time_date':'review_time'})
        sort_array = ['queue', 'review_time', 'grupo_revision',
                      'tipo_accion', 'rango_tiempo_revision']
        group_columns = ['review_time', 'tipo_accion', 'grupo_revision',
                         'queue', 'rango_tiempo_revision']

        df = df[group_columns].sort_values(by=sort_array)

        df['ads_revisados'] = df.groupby(group_columns)['review_time']\
                                .transform('size')

        df = df.drop_duplicates().reset_index()

        df = df[['queue', 'review_time',
                 'rango_tiempo_revision', 'ads_revisados']]
        df['suma'] = df.groupby(['review_time', 'queue',
                                 'rango_tiempo_revision']).transform('sum')
        df = df.drop_duplicates().reset_index(drop=True)

        df['total_ads'] = df[['review_time', 'queue', 'ads_revisados']]\
                            .groupby(['review_time', 'queue']).transform('sum')
        df = df.drop_duplicates().reset_index(drop=True)

        df = df[['review_time', 'queue',
                 'rango_tiempo_revision', 'suma', 'total_ads']]\
                .pivot_table('suma',
                             ['review_time', 'queue', 'total_ads'],
                             'rango_tiempo_revision')

        df = self.complete_rows(df.reset_index())\
                 .rename(columns=self.dict_columns)

        lmd_perc = lambda x, y: np.around(x*100/y, decimals=2)
        df['< 5 min'] = df.apply(
            lambda x: lmd_perc(x['avisos - < 5 min'], x['total_ads']),
            axis=1)
        df['(5:00 - 14:59)min'] = df.apply(
            lambda x: lmd_perc(x['avisos - 5:00 - 14:59min'], x['total_ads']),
            axis=1)
        df['(15:00 - 29:59)min'] = df.apply(
            lambda x: lmd_perc(x['avisos - 15:00 - 29:59min'], x['total_ads']),
            axis=1)
        df['(30:00 - 44:59)min'] = df.apply(
            lambda x: lmd_perc(x['avisos - 30:00 - 44:59min'], x['total_ads']),
            axis=1)
        df['(45:00 - 59:59)min'] = df.apply(
            lambda x: lmd_perc(x['avisos - 45:00 - 59:59min'], x['total_ads']),
            axis=1)
        df['(1:00 - 1:30)hrs'] = df.apply(
            lambda x: lmd_perc(x['avisos - 1:00 - 1:30hrs'], x['total_ads']),
            axis=1)
        df['(1:31 - 2:00)hrs'] = df.apply(
            lambda x: lmd_perc(x['avisos - 1:31 - 2:00hrs'], x['total_ads']),
            axis=1)
        df['(2:01 - 3:00)hrs'] = df.apply(
            lambda x: lmd_perc(x['avisos - 2:01 - 3:00hrs'], x['total_ads']),
            axis=1)
        df['(3:01 - 4:00)hrs'] = df.apply(
            lambda x: lmd_perc(x['avisos - 3:01 - 4:00hrs'], x['total_ads']),
            axis=1)
        df['> 4 hrs'] = df.apply(
            lambda x: lmd_perc(x['avisos -> 4 hrs'], x['total_ads']),
            axis=1)
        return df[self.columns_final]

    def send_email(self):
        self.logger.info('Preparing email')
        SUBJECT = "SLA Yapo"
        if self.params.email_from is None:
            FROM = "noreply@yapo.cl"
        else:
            FROM = self.params.email_from
        if self.params.email_to == []:
            TO = ['data_team@adevinta.com']
        else:
            TO = self.params.email_to
        BODY = """Estimad@s,
Se adjuntan estadísticas de revisión al día de ayer.\n\nSaludos,\nBI Team
-----
Este correo ha sido generado de forma automática."""
        msg = MIMEMultipart('mixed')
        msg['Subject'] = SUBJECT
        msg['From'] = FROM
        msg['To'] = ", ".join(TO)
        textPart = MIMEText(BODY, 'plain')
        msg.attach(textPart)
        part = MIMEBase('application', "octet-stream")
        part.set_payload(open(self.file_name, "rb").read())
        encode_base64(part)
        part.add_header('Content-Disposition',
                        'attachment', filename=self.file_name)
        msg.attach(part)
        logger_send_mail = 'Sending email to {}'.format(", ".join(TO))
        self.logger.info(logger_send_mail)
        server = smtplib.SMTP('127.0.0.1')
        server.sendmail(FROM, TO, msg.as_string())
        self.logger.info('Email sent')

    def generate(self):
        self.data_review_ads_base = self.config.db
        df = self.getting_reviews_time_per_queue(
            self.data_review_ads_base.copy())
        df.to_excel(self.file_name)
        self.send_email()
