# pylint: disable=no-member
# utf-8
import sys
import logging
import pandas as pd
import re
from infraestructure.conf import getConf
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams
from utils.time_execution import TimeExecution


# Method that allow applied fix to dataframe
def transform_dataframe(data_packs: pd.DataFrame,
                        type_fix: str,
                        column_old: str=None,
                        column_new: str=None) -> pd.DataFrame:
    if type_fix == 'rename':
        if column_old in data_packs and column_new is not None:
            data_packs = data_packs.rename(columns={column_old: column_new})
    if type_fix == 'drop':
        if column_old in data_packs:
             data_packs = data_packs.drop(columns=[column_old])
    return data_packs

# Read csv file of manual packs
def read_csv_manual_packs(config: getConf) -> pd.DataFrame:
    data_packs = pd.read_csv(config.csv.path)
    data_packs['pack_id'] = 1111
    data_packs['account_id'] = 11111111
    # Rename columns
    data_packs = transform_dataframe(data_packs,
                                     'rename',
                                     'Fecha de Inicio',
                                     'date_start')
    data_packs = transform_dataframe(data_packs,
                                     'rename',
                                     'Fecha de Termino',
                                     'date_end')
    data_packs = transform_dataframe(data_packs,
                                     'rename',
                                     'N° Cupos (Clasificados)',
                                     'slots')
    data_packs = transform_dataframe(data_packs,
                                     'rename',
                                     'Integrador',
                                     'tipo_pack')
    #Drop unsed columns
    data_packs = transform_dataframe(data_packs,
                                     'drop',
                                     'Nombre de la propuesta')
    data_packs = transform_dataframe(data_packs,
                                     'drop',
                                     'Punto de Venta')
    data_packs = transform_dataframe(data_packs,
                                     'drop',
                                     'Plazo')
    data_packs = transform_dataframe(data_packs,
                                     'drop',
                                     'Periodo fiscal')
    data_packs = transform_dataframe(data_packs,
                                     'drop',
                                     'Fecha Estimada de Cierre')
    data_packs = transform_dataframe(data_packs,
                                     'drop',
                                     'Etapa')
    data_packs = transform_dataframe(data_packs,
                                     'drop',
                                     'Mail de activación')
    data_packs = transform_dataframe(data_packs,
                                     'drop',
                                     'Propietario de la propuesta')
    data_packs = transform_dataframe(data_packs,
                                     'drop',
                                     'N° Factura')
    data_packs = transform_dataframe(data_packs,
                                     'drop',
                                     'Antigüedad')
    data_packs = transform_dataframe(data_packs,
                                     'drop',
                                     'Meta')
    data_packs = transform_dataframe(data_packs,
                                     'drop',
                                     'Función del propietario')
    data_packs = transform_dataframe(data_packs,
                                     'drop',
                                     'Fecha de creación')
    data_packs = transform_dataframe(data_packs,
                                     'drop',
                                     'Código Propuesta')
    data_packs['doc_num'] = data_packs['N° Orden de Compra'].str.split("-", n = 2, expand = True)[1].astype('int32')
    data_packs = transform_dataframe(data_packs,
                                     'drop',
                                     'N° Orden de Compra')
    data_packs['price'] = round(data_packs['Monto Neto (Sin IVA)'].str.split("$", n = 2, expand = True)[1].astype('str').str.replace(',','').astype('int32')*1.19,0)
    data_packs = transform_dataframe(data_packs,
                                     'drop',
                                     'Monto Neto (Sin IVA)')
    # Transformation category field
    data_real_estate = data_packs.loc[data_packs.Sector == 'Inmuebles']
    data_real_estate['category'] = 'real_estate'
    data_real_estate = transform_dataframe(data_real_estate,
                                           'drop',
                                           'Sector')
    data_cars = data_packs.loc[data_packs.Sector == 'Vehículos']
    data_cars['category'] = 'car'
    data_cars = transform_dataframe(data_cars,
                                    'drop',
                                    'Sector')
    data_packs = data_real_estate
    data_packs = data_packs.append(data_cars).reset_index(drop=True)
    # Transform data type for date fields.
    data_packs['date_end'] =  pd.to_datetime(data_packs['date_end'], format='%d-%m-%Y')
    data_packs['date_start'] =  pd.to_datetime(data_packs['date_start'], format='%d-%m-%Y')
    return data_packs

# method that allow mapping product name define in csv with store in database
def transform_product_name(product_name: str=None) -> str:
    if product_name.lower() == 'PACK INMOBILIARIA 50/ANUAL'.lower():
        product_name = 'Pack Inmo 50 anual'
    elif product_name.lower() == 'PACK INMOBILIARIA 75/ANUAL'.lower():
        product_name = 'Pack Inmo 75 anual'
    elif product_name.lower() == 'PACK INMOBILIARIA 100/ANUAL'.lower():
        product_name = 'Pack Inmo 100 anual'
    elif product_name.lower() == 'PACK INMOBILIARIA 125/ANUAL'.lower():
        product_name = 'Pack Inmo 125 anual'
    return product_name

# Method that allow create a email from account name.
def create_email(account_name: str=None) -> str:
    account_name = account_name.lower()
    if 'scp' in account_name:
        account_name = account_name.replace('scp', '')
    if '-' in account_name:
        account_name = account_name.replace('-', '')
    if '(' in account_name:
        account_name = account_name.replace('(', '')
    if ')' in account_name:
        account_name = account_name.replace(')', '')
    accounta_name_init = account_name
    div_account = account_name.split(' ')
    if len(div_account) == 1:
        account_name = account_name + '@' + account_name + '.cl'
    else:
        email = ''
        middle = int(len(div_account) / 2)
        for i in range(0, middle):
            email = email + div_account[i]
        email = email + '@'
        for i in range(middle, len(div_account)):
            email = email + div_account[i]
        email = email + '.cl'
        account_name = email
    match = re.findall('\S+@\S+.cl', account_name)
    if len(match) == 0:
        accounta_name_init = accounta_name_init.replace(' ', '')
        account_name = accounta_name_init + '@' + accounta_name_init + '.cl'
    return account_name

# Method that allow assign a product_id to manual pack.
def transform_product_packs(data_packs: pd.DataFrame,
                            data_product: pd.DataFrame) -> pd.DataFrame:
    product_id_list = []
    days_list = []
    email_list = []
    for index, row in data_packs.iterrows():
        # Get product name
        product_name = transform_product_name(row['Nombre del producto'])
        df_product_id = data_product.loc[data_product.product_name == product_name]
        df_product_id = df_product_id['product_id_nk'].astype('int32').reset_index(drop=True)
        product_id_list.append(df_product_id[0])
        # Get interval days
        days = (row['date_end'] - row['date_start']).days
        days_list.append(days)
        # Get email
        email = create_email(row['Nombre de la cuenta'])
        email_list.append(email)
    # Add column product_id
    df_product_id = pd.DataFrame(product_id_list)
    column_count = data_packs.shape[1]
    data_packs.insert(column_count, 'product_id', df_product_id)
    data_packs = transform_dataframe(data_packs,
                                     'drop',
                                     'Nombre del producto')
    # Add column days
    df_days = pd.DataFrame(days_list)
    column_count = data_packs.shape[1]
    data_packs.insert(column_count, 'days', df_days)
    # Add column email
    df_email = pd.DataFrame(email_list)
    column_count = data_packs.shape[1]
    data_packs.insert(column_count, 'email', df_email)
    # Drop 'Nombre de la cuenta' column
    data_packs = transform_dataframe(data_packs,
                                     'drop',
                                     'Nombre de la cuenta')
    return data_packs

# Query data from data warehouse
def source_data_dwh(params: ReadParams,
                    config: getConf):
    query = Query(config, params)
    db_source = Database(conf=config.db)
    data_dwh = db_source.select_to_dict(query \
                                        .query_product())
    db_source.close_connection()
    return data_dwh

# Query data from data warehouse
def source_pivot(params: ReadParams,
                    config: getConf):
    query = Query(config, params)
    db_source = Database(conf=config.db)
    data_dwh = db_source.select_to_dict(query \
                                        .query_pivot())
    db_source.close_connection()
    return data_dwh

# Write data to data warehouse
def write_data_dwh(params: ReadParams,
                   config: getConf,
                   data_packs: pd.DataFrame) -> None:
    query = Query(config, params)
    DB_WRITE = Database(conf=config.db)
    DB_WRITE.execute_command(query.delete_pivot())
    DB_WRITE.insert_data_pivot(data_packs)
    DB_WRITE.close_connection()

if __name__ == '__main__':
    CONFIG = getConf()
    TIME = TimeExecution()
    LOGGER = logging.getLogger('manual-packs')
    DATE_FORMAT = """%(asctime)s,%(msecs)d %(levelname)-2s """
    INFO_FORMAT = """[%(filename)s:%(lineno)d] %(message)s"""
    LOG_FORMAT = DATE_FORMAT + INFO_FORMAT
    logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
    PARAMS = ReadParams(sys.argv)
    DATA_PACKS = read_csv_manual_packs(CONFIG)
    DATA_PRODUCT = source_data_dwh(PARAMS, CONFIG)
    DATA_PACKS = transform_product_packs(DATA_PACKS, DATA_PRODUCT)
    write_data_dwh(PARAMS, CONFIG, DATA_PACKS)
    DATA_PIVOT_PACKS = source_pivot(PARAMS, CONFIG)
    print(DATA_PIVOT_PACKS.head(20))
    TIME.get_time()
    LOGGER.info('Process ended successfully.')
