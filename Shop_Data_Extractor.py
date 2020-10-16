from sqlalchemy import create_engine, text
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy import inspect
from clickhouse_sqlalchemy import Table, engines
from clickhouse_driver import Client

import pandas as pd
import re
from datetime import date
import datetime
import calendar
from datetime import date
from datetime import timedelta, timezone
from logger import Logger

class Shop_Data_Extractor :
    def __init__(self, maria_id, maria_password):


        self.Maria_id = maria_id
        self.Maria_password = maria_password
        self.inner_logger = Logger()
        self.connect_db()

    def connect_db(self):
        self.MariaDB_Engine = create_engine('mysql+pymysql://{0}:{1}@192.168.100.108:3306/dreamsearch'
                                            .format(self.Maria_id, self.Maria_password))
        self.MariaDB_Engine_Conn = self.MariaDB_Engine.connect()

    def return_adver_id_list(self):
        self.connect_db()
        adver_id_list_sql = """
        SELECT 
        distinct ADVER_ID
        FROM 
        dreamsearch.ADVER_PRDT_CATE_INFO;
        """
        sql_text = text(adver_id_list_sql)
        ADVER_ID_LIST = list(pd.read_sql(sql_text, self.MariaDB_Engine_Conn))
        return ADVER_ID_LIST

    def extract_product_price_info(self,ADVER_ID_LIST):
        self.connect_db()
        price_info_list = []
        for ADVER_ID in ADVER_ID_LIST :
            price_info_sql = """
            select USERID as ADVER_ID, PCODE,PNM, PRICE
            from dreamsearch.SHOP_DATA
            where USERID = '{0}';
            """.format(ADVER_ID)
            sql_text = text(price_info_sql)
            price_info_list.append(pd.read_sql(sql_text, self.MariaDB_Engine_Conn))
