# pylint: disable=no-member
# utf-8
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.encoders import encode_base64
import pandas as pd
from utils.query import Query
from utils.read_params import ReadParams
from infraestructure.psql import Database

class SendEmailSellersPackLeak():

    def __init__(self, config, params: ReadParams) -> None:
        self.config = config
        self.params = params
        self.logger = logging.getLogger('Seller-pack-leack')
        self.file_name = 'sellers_pack_fuga.csv'
        self.db = None
        self.db_dev = None

    @property
    def data_sellers_leack(self):
        return self.__data_sellers_leack

    @data_sellers_leack.setter
    def data_sellers_leack(self, config):
        query = Query(config, self.params)
        if self.db is None:
            self.db = Database(conf=config.db)
        self.logger.info('Making Query')
        data = pd.read_sql(sql=query.query_sellers_pack_leak(),
                           con=self.db.connection)
        self.logger.info('Query Ended')
        self.__data_sellers_leack = data
        self.db.close_connection()

    def send_email(self):
        SUBJECT = "Base de correos: Fuga de sellers pack"
        FROM = "bi_team@schibsted.cl"
        #TO = ['claudia@schibsted.cl','experiencia@yapo.cl',
        #'sofia@schibsted.cl','bi@schibsted.cl','constanza@schibsted.cl']
        TO = ['ricardo.alvarez@adevinta.com']
        CSV_FILE = self.file_name
        BODY = """
            Estimad@s,

            Se adjunta base de correos con fuga de sellers pack del mes anterior.

            Quedamos atentos por cualquier duda o consulta.

            Saludos,
            BI Team
            -----
            Este correo ha sido generado de forma autom\xE1tica
        """
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
                        'attachment; filename=Sellers_Pack_Fuga.csv')
        msg.attach(part)
        server = smtplib.SMTP('localhost')
        server.sendmail(FROM, TO, msg.as_string())

    def generate(self):
        self.data_sellers_leack = self.config
        self.data_sellers_leack.to_csv(self.file_name, sep=";")
        self.send_email()
        