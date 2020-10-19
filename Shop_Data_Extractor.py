from sqlalchemy import create_engine, text
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy import inspect
from clickhouse_sqlalchemy import Table, engines
from clickhouse_driver import Client

import pandas as pd
import re
from datetime import date, datetime, timedelta, timezone
import calendar
from dateutil.tz import tzlocal
from logger import Logger
import argparse


class Shop_Data_Extractor :
    def __init__(self, maria_id, maria_password, local_clickhouse_id,
                                    local_clickhouse_password,
                 local_clickhouse_db_name ):

        self.Maria_id = maria_id
        self.Maria_password = maria_password

        self.Clickhouse_id = local_clickhouse_id
        self.Clickhouse_password = local_clickhouse_password
        self.Clickhouse_DB = local_clickhouse_db_name

        local_tz = tzlocal()
        now_time = datetime.now(tz=local_tz).strftime("%Y%m%d%H")
        print(now_time)
        self.inner_logger = Logger(now_time, "shop_data_context_"+now_time+'.json')
        self.connect_db()
        self.connect_local_clickhouse_db()

    def connect_db(self):
        self.MariaDB_Engine = create_engine('mysql+pymysql://{0}:{1}@192.168.100.106:3306/dreamsearch'
                                            .format(self.Maria_id, self.Maria_password))
        self.MariaDB_Engine_Conn = self.MariaDB_Engine.connect()
        return True

    def connect_local_clickhouse_db(self):
        self.Local_Click_House_Engine = create_engine(
            'clickhouse://{0}:{1}@localhost/{2}'.format(self.Clickhouse_id, self.Clickhouse_password,
                                                        self.Clickhouse_DB))
        self.Local_Click_House_Conn = self.Local_Click_House_Engine.connect()
        print(self.Local_Click_House_Conn)
        return True

    def create_shop_property_table(self,table_name):
        client = Client(host='localhost')
        DDL_sql = """
         CREATE TABLE IF NOT EXISTS {0}.{1}
         (
            ADVER_ID String,
            PCODE String,
            PRODUCT_CATE_NO Nullable(String),
            FIRST_CATE Nullable(String),
            SECOND_CATE Nullable(String),
            THIRD_CATE Nullable(String),
            PNM Nullable(String),
            PRICE Nullable(String)
         ) ENGINE = MergeTree
         PARTITION BY ADVER_ID
         ORDER BY (ADVER_ID, PCODE)
         SAMPLE BY ADVER_ID
         SETTINGS index_granularity=8192
         """.format(self.Clichouse_DB, table_name)
        result = client.execute(DDL_sql)
        return result

    def return_adver_id_list(self):
        self.connect_db()
        adver_id_list_sql = """
        SELECT 
        distinct ADVER_ID
        FROM 
        dreamsearch.ADVER_PRDT_CATE_INFO;
        """
        sql_text = text(adver_id_list_sql)
        ADVER_ID_LIST = pd.read_sql(sql_text, self.MariaDB_Engine_Conn)['ADVER_ID']
        return ADVER_ID_LIST

    def extract_product_cate_info(self):
        self.connect_db()
        product_cate_info_sql = """
        SELECT 
        apci.ADVER_ID,
        apci.PRODUCT_CODE as PCODE,
        apci.ADVER_CATE_NO as PRODUCT_CATE_NO,
        apsc.FIRST_CATE,
        apsc.SECOND_CATE,
        apsc.THIRD_CATE
        FROM dreamsearch.ADVER_PRDT_CATE_INFO as apci
        join
        (select * 
        from 
        dreamsearch.ADVER_PRDT_STANDARD_CATE) as apsc
        on apci.ADVER_CATE_NO = apsc.no;
        """
        text_sql = text(product_cate_info_sql)
        self.product_cate_info_df = pd.read_sql(text_sql, self.MariaDB_Engine_Conn)
        return True

    def extract_product_price_info(self, ADVER_ID_LIST, Table_name):
        self.connect_db()
        self.connect_local_clickhouse_db()
        product_property_df_list = []
        size = ADVER_ID_LIST.shape[0]
        data_count = 0
        now_time = datetime.now(tz= tzlocal()).strftime("%Y%m%d%H")
        print("Start_time :", now_time)
        for i, ADVER_ID in enumerate(ADVER_ID_LIST) :
            price_info_sql = """
            SELECT 
            USERID as ADVER_ID,
            PCODE,PNM,
            PRICE
            FROM dreamsearch.SHOP_DATA
            WHERE USERID = '{0}';
            """.format(ADVER_ID)
            sql_text = text(price_info_sql)
            try :
                product_price_info_df = pd.read_sql(sql_text, self.MariaDB_Engine_Conn)
                merged_df = pd.merge(self.product_cate_info_df, product_price_info_df,on=['ADVER_ID','PCODE'])
                product_property_df_list.append(merged_df)
            except :
                self.connect_db()
                self.connect_local_clickhouse_db()
                product_price_info_df = pd.read_sql(sql_text, self.MariaDB_Engine_Conn)
                merged_df = pd.merge(self.product_cate_info_df, product_price_info_df, on=['ADVER_ID', 'PCODE'])
                product_property_df_list.append(merged_df)
            if i % 10 == 0 :
                print("{0}/{1} : ".format(i,size),ADVER_ID)
        now_time = datetime.now(tz=tzlocal()).strftime("%Y%m%d%H")
        final_df = pd.concat(product_property_df_list)
        print("End time : ", now_time)
        final_df.to_sql(Table_name, con=self.Local_Click_House_Engine, index = False, if_exists = 'replace')
        return True

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--create_table", help="--add_table ~~ ", default = None )
    args = parser.parse_args()

    # for develop
    # maria_id = "dyyang"
    # maria_password = "dyyang123!"

    # for service
    logger_name = input("logger name is : ")
    logger_file = input("logger file name is : ")
    maria_id = "analysis"
    maria_password = "analysis@2020"
    click_house_id = "click_house_test1"
    click_house_password = "0000"
    click_house_DB = "TEST"

    shop_data_context = Shop_Data_Extractor(maria_id, maria_password,
                                            click_house_id, click_house_password, click_house_DB)

    logger = Logger(logger_name, logger_file)
    logger.log("create_table", args.create_table.upper())
    table_name = input("New table name : ")

    if args.create_table == 'NEW_TABLE' :
        shop_data_context.create_shop_property_table(table_name)
        logger.log("create clickhouse table {0} success".format(table_name), 'True')
    else :
        pass

    adver_id_list = shop_data_context.return_adver_id_list()
    print(adver_id_list)
    product_cate_info = shop_data_context.extract_product_cate_info()
    print(shop_data_context.product_cate_info_df)
    extract_product_price_info = shop_data_context.extract_product_price_info(adver_id_list, table_name)
    print(extract_product_price_info)
