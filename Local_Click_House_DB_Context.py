from sqlalchemy import create_engine, text
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy import inspect
from clickhouse_sqlalchemy import Table, engines
from clickhouse_driver import Client

# ip to region data library
import requests
import numpy
import json
import random
import socket
import struct
from geopy.geocoders import Nominatim
from global_land_mask import globe

import pandas as pd
import re
from datetime import date
import datetime
import calendar
from datetime import date
from datetime import timedelta, timezone
from logger import Logger

class Local_Click_House_DB_Context :
    def __init__(self, Local_Clickhouse_Id, Local_Clickhouse_password, Local_Clickhouse_Ip,
                 DB_NAME, TABLE_NAME):

        self.Local_Clickhouse_Id = Local_Clickhouse_Id
        self.Local_Clickhouse_password = Local_Clickhouse_password
        self.Local_Clickhouse_Ip = Local_Clickhouse_Ip
        self.DB_NAME = DB_NAME
        self.TABLE_NAME = TABLE_NAME
        self.connect_db()
        # print("ADVER_CATE_INFO Function", self.Extract_Adver_Cate_Info())
        # print("MEDIA_Property_info Function",self.Extract_Media_Property_Info())
        # print("Product_property_info Function ", self.Extract_Product_Property_Info())

    def connect_db(self) :
        self.Local_Click_House_Engine = create_engine('clickhouse://{0}:{1}@{2}/{3}'.format(self.Local_Clickhouse_Id,
                                                                                            self.Local_Clickhouse_password,
                                                                                            self.Local_Clickhouse_Ip,
                                                                                            self.DB_NAME))
        self.Local_Click_House_Conn = self.Local_Click_House_Engine.connect()

        return True

    def Extract_Adver_Cate_Info(self):
        self.connect_db()
        try:
            Adver_Cate_Df_sql = """
                    SELECT
                    *
                    FROM
                    TEST.ADVER_PROPERTY_INFO
                 """
            Adver_Cate_Df_sql = text(Adver_Cate_Df_sql)
            self.Adver_Cate_Df = pd.read_sql(Adver_Cate_Df_sql, self.Local_Click_House_Conn)
            return True
        except:
            print("Extract_Adver_Cate_Info error happend")
            return False

    def Extract_Media_Property_Info(self) :
        self.connect_db()
        try:
            Media_Property_sql = """
                    SELECT
                    *
                    FROM
                    TEST.MEDIA_PROPERTY_INFO
                 """
            Media_Property_sql = text(Media_Property_sql)
            self.Media_Property_Df = pd.read_sql(Media_Property_sql, self.Local_Click_House_Conn)
            return True
        except:
            print("Extract_Adver_Cate_Info error happend")
            return False

    def Extract_Product_Property_Info(self):
        self.connect_db()
        try :
            PRODUCT_PROPERTY_INFO_sql = """
            SELECT
            *
            FROM
            TEST.SHOP_PROPERTY_INFO
            """
            sql_text = text(PRODUCT_PROPERTY_INFO_sql)
            self.Product_Property_Df = pd.read_sql(sql_text,self.Local_Click_House_Conn)
            return True
        except : 
            return False

    def Extract_Click_View_Log (self, start_dttm, last_dttm, Adver_Info = False,
                                Media_Info = False, Product_Info = False, data_size = 1000000) :
        self.connect_db()
        return_df_list = []
        data_cnt = 0
        rotation_cnt = int(data_size/10000)
        while data_cnt < data_size :
            sampling_parameter = random.random()
            minute_sample_parameter = random.randint(0,60)
            Extract_Data_sql = """
            SELECT * FROM
            {0}
            sample {1}
            where STATS_DTTM = {2}
            and STATS_MINUTE = {3}
            limit 
            """.format(self.DB_NAME+'.'+self.TABLE_NAME, sample_size, stats_dttm, stats_hh, data_size )
            sql_text = text(Extract_Data_sql)
            sql_result = pd.read_sql(sql_text, self.Local_Click_House_Conn)
            print(Extract_Data_sql)
            sql_text = text(Extract_Data_sql)
            sql_result = pd.read_sql(sql_text,self.Local_Click_House_Conn)
        if Adver_Info == True :
            sql_result = pd.merge(sql_result, self.Adver_Cate_Df, on=['ADVER_ID'],how='left')
        if Media_Info == True :
            sql_result = pd.merge(sql_result, self.Media_Property_Df, on=['MEDIA_SCRIPT_NO'],how='left')
        if Product_Info == True :
            sql_result = pd.merge(sql_result, self.Product_Property_Df, on=['PCODE'], how = 'left')
        return sql_result

    def getting_ip(self, row):
        """This function calls the api and return the response"""
        url = f"https://freegeoip.app/json/{row}"  # getting records from getting ip address
        headers = {
            'accept': "application/json",
            'content-type': "application/json"
        }
        response = requests.request("GET", url, headers=headers)
        respond = json.loads(response.text)
        return respond

    def check_table_name(self,table_name ) :
        self.connect_db()
        check_table_name_sql = """
            SHOW TABLES FROM {0}
        """.format(self.DB_NAME)
        sql_text = text(check_table_name_sql)
        sql_result = list(pd.read_sql(sql_text, self.Local_Click_House_Conn)['name'])
        print(sql_result)
        if table_name in sql_result :
            return True
        else :
            return False
                
if __name__ == "__main__" :

    # def __init__(self, maria_id, maria_password,
    #              Local_Clickhouse_Id, Local_Clickhouse_password, Local_Clickhouse_Ip,
    #              DB_NAME, TABLE_NAME):

    # test용 property data
    logger_name = "test"
    logger_file = "test.json"
    clickhouse_id = "analysis"
    clickhouse_password = "analysis@2020"
    maria_id = "dyyang"
    maria_password = "dyyang123!"
    local_clickhouse_id = "click_house_test1"
    local_clickhouse_password = "0000"
    local_clickhouse_ip = "192.168.100.237:8123"
    local_clickhouse_DB_name = "TEST"
    local_clickhouse_Table_name = 'CLICK_VIEW_YN_LOG'
    test_context = Local_Click_House_DB_Context(local_clickhouse_id,local_clickhouse_password,
                                                local_clickhouse_ip,local_clickhouse_DB_name,
                                                local_clickhouse_Table_name)
    log_data = test_context.Extract_Click_View_Log('20200926',sample_size = 0.1, data_size=100000)
    print(log_data)