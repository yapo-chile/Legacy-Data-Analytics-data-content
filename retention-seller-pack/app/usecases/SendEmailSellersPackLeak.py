# pylint: disable=no-member
# utf-8
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.encoders import encode_base64
import pandas as pd
from utils.query_seller_leak import QuerySellerLeak
from utils.read_params import ReadParams
from infraestructure.psql import Database

class SendEmailSellersPackLeak():

    def __init__(self, config, params: ReadParams) -> None:
        self.config = config
        self.params = params
        self.logger = logging.getLogger('Seller-pack-leack')
        self.file_name = 'sellers_pack_fuga.csv'

    @property
    def data_sellers_leack(self):
        return self.__data_sellers_leack

    @data_sellers_leack.setter
    def data_sellers_leack(self, config):
        query = QuerySellerLeak(config, self.params)
        db = Database(conf=config.db)
        self.logger.info('Executing query to get data from dwh')
        data = pd.read_sql(sql=query.query_sellers_pack_leak(),
                           con=db.connection)
        self.logger.info('Query executed')
        db.close_connection()
        self.__data_sellers_leack = data

    def send_email(self):
        self.logger.info('Preparing email')
        SUBJECT = "Base de correos: Fuga de sellers pack"
        FROM = "noreply@yapo.cl"
        #TO = ['claudia@schibsted.cl','experiencia@yapo.cl',
        #'sofia@schibsted.cl','bi@schibsted.cl','constanza@schibsted.cl']
        TO = ['ricardo.alvarez@adevinta.com']
        CSV_FILE = self.file_name
        BODY = """Estimad@s,\n\tSe adjunta base de correos con fuga de sellers pack del mes anterior.
Quedamos atentos por cualquier duda o consulta.\n\nSaludos,\nBI Team"""
        msg = MIMEMultipart('mixed')
        msg['Subject'] = SUBJECT
        msg['From'] = FROM
        msg['To'] = ", ".join(TO)
        textPart = MIMEText(BODY, 'plain')
        msg.attach(textPart)
        part = MIMEBase('application', "octet-stream")
        part.set_payload(open(CSV_FILE, "rb").read())
        encode_base64(part)
        part.add_header('Content-Disposition',
                        'attachment',filename=self.file_name)
        msg.attach(part)
        self.logger.info('Sending email')
        server = smtplib.SMTP('localhost')
        server.sendmail(FROM, TO, msg.as_string())
        self.logger.info('Email sent')


    def generate(self):
        self.data_sellers_leack = self.config
        self.data_sellers_leack.to_csv(self.file_name, sep=";")
        self.send_email()
        