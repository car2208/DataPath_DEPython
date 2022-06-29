import pandas as pd
import numpy as np
import psycopg2
import mysql.connector
import sqlite3
from datetime import date,datetime,timedelta
from dateutil.relativedelta import relativedelta
aws_credentials = { "key": "AKIAYAFNZI7E56ZYMHML", "secret": "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" }

def conexionbd(flagbd):

    if flagbd =='mysql':
        mydb = mysql.connector.connect(host="localhost",user="root",password="toor",database="datapath")
        conn=mydb

    if flagbd =='possql':
        cad = psycopg2.connect(database = "datapath", user = "postgres", password = "toor", host = "localhost", port = "5432")
        conn=cad
    
    if flagbd =='sqlite':
        mydb = conn = sqlite3.connect('C:\\Users\\CCHAVARRY\\datapath.db')
        conn=mydb

    return conn


def ultimosMeses(meses):
    hoy=date.today()
    fecha=hoy.replace(day=1)
    month = relativedelta(months=meses)
    month1 = relativedelta(months=1)
    hoy_menos_meses = fecha - month
    hoy_menos_uno = fecha - month1
    periodo_inicio=hoy_menos_meses.year*100+hoy_menos_meses.month
    periodo_fin=hoy_menos_meses.year*100+hoy_menos_uno.month
    list_meses_formats=[hoy_menos_meses,fecha,periodo_inicio,periodo_fin]
    
    return list_meses_formats


###################EXTRACT################################

def extract_table_query(connection,query):
    df=pd.read_sql(query, con=connection)
    return df

def extract_products():
    name_cabeceras=["product_id","product_category_id","product_name","product_description","product_price","product_image"]
    df_products = pd.read_csv("s3://datapathcchb/products", storage_options=aws_credentials, sep="|", header=None, names=name_cabeceras)
    return df_products

def extract_departments():
    name_cabe_depart=["departament_id","departament_name"]
    df_departments = pd.read_csv("s3://datapathcchb/departments", storage_options=aws_credentials, sep="|", header=None, names=name_cabe_depart)
    return df_departments

def extract_orders():

    cadena_mysql=conexionbd("mysql")
    sql_orders="select * from orders2"
    orders=extract_table_query(cadena_mysql,sql_orders)
    return orders

def extract_order_items():

    cadena_mysql=conexionbd("mysql")
    sql_order_items="select * from order_items"
    order_items=extract_table_query(cadena_mysql,sql_order_items)
    return order_items

def extract_categories():
    cadena_possql=conexionbd("possql")
    sql_categories="select * from categories"
    categories=extract_table_query(cadena_possql,sql_categories)
    return categories

def extract_customer():
    cadena_possql=conexionbd("possql")
    sql_customer="select * from customer"
    customer=extract_table_query(cadena_possql,sql_customer)
    return customer

#########################################################################

###############TRANSFORMACIONES Y CALCULOS ################################

def calcular_categoria(df_orders,df_order_items,df_customer,df_products,df_departments,df_categories):

     ####################################################################
    fechas=ultimosMeses(6)
    fec_ini=fechas[0]
    fec_fin=fechas[1]
    per_ini=fechas[2]
    per_fin=fechas[3]
    print (fec_ini)
    print (fec_fin)
    print (per_ini)
    print (per_fin)
    
    df2_orders=df_orders[ (df_orders["order_date"] >=fec_ini) &\
                       (df_orders["order_date"] < fec_fin) &\
                       (df_orders["order_staturs"] == 'COMPLETE') ]

    df2_order_items=df_order_items[["order_item_order_id","order_item_subtotal"]]\
    .groupby("order_item_order_id", as_index=False)\
    .agg({"order_item_subtotal": "sum"})

    df_orderjoinitems=pd.merge(df2_orders,df2_order_items , how='inner',left_on='order_id',right_on='order_item_order_id')

    df_customer_group=df_orderjoinitems[["order_customer_id","order_item_subtotal"]]\
    .groupby("order_customer_id", as_index=False)\
    .agg({"order_item_subtotal": "sum"})

    df_customer_group=df_customer_group.assign(per_ini=per_ini)
    df_customer_group=df_customer_group.assign(per_fin=per_fin)

    df_customer_group['cod_category'] = \
    np.where((df_customer_group['order_item_subtotal']>0) & (df_customer_group['order_item_subtotal']<=950), 'A',
    np.where((df_customer_group['order_item_subtotal']>950) & (df_customer_group['order_item_subtotal']<=1500), 'B',
    np.where((df_customer_group['order_item_subtotal']>1500) & (df_customer_group['order_item_subtotal']<=2500), 'C', 
    np.where((df_customer_group['order_item_subtotal']>2500) & (df_customer_group['order_item_subtotal']<=3500), 'D',
    np.where((df_customer_group['order_item_subtotal']>3500) & (df_customer_group['order_item_subtotal']<=5000), 'E',
    np.where((df_customer_group['order_item_subtotal']>5000), 'E','-'
    ))))))

    df_customer_group = df_customer_group.rename(columns=
    {
     'order_customer_id': 'cod_customer',
     'order_item_subtotal': 'mto_compras' 
    })

    return df_customer_group

##################### LOAD ######################################

def load_target(target,df):
    
    conection_target=conexionbd("sqlite")
    df.to_sql(target,
    con=conection_target, 
    if_exists="replace", 
    index=False)

###################################################################

def log(message):
    timestamp_format = '%H:%M:%S-%h-%d-%Y'
    #Hour-Minute-Second-MonthName-Day-Year
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("C:\\Users\\CCHAVARRY\\Desktop\\ProyectoDataPath\\proyecto\\logfile.txt","a") as f: f.write(timestamp + ',' + message + '\n')
    
def main():

    log("ETL Job Started")

    #############################
    log("--Extract phase Started")
    
    log("----Extract orders Started")
    df_orders=extract_orders()
    log("----Extract orders Ended")
    
    log("----Extract Order Items Started")
    df_order_items=extract_order_items()
    log("----Extract Order Item Ended")
    
    log("----Extract Customer Started")
    df_customer=extract_customer()
    log("----Extract Customer Ended")

    log("----Extract Products Started")
    df_products=extract_products()
    log("----Extract Products Ended")

    log("----Extract Departments Started")
    df_departments=extract_departments()
    log("----Extract Departments Ended")

    log("----Extract Categories Started")
    df_categories=extract_categories()
    log("----Extract Categories Ended")

    log("--Extract phase Ended")

    ##############################
    log("--Transform phase Started")
    target = "customer_category"
    dftransf=calcular_categoria(df_orders,df_order_items,df_customer,df_products,df_departments,df_categories)
    log("--Transform phase Ended")

    ##############################
    log("--Load phase Started")
    load_target(target,dftransf)
    log("--Load phase Ended")

    log("ETL Job Ended")

if __name__ == "__main__":
   main()