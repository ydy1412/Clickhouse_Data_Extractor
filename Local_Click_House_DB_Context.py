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


class Local_Click_House_DB_Context:
    def __init__(self, Local_Clickhouse_Id, Local_Clickhouse_password, Local_Clickhouse_Ip,
                 DB_NAME, TABLE_NAME):

        self.Local_Clickhouse_Id = Local_Clickhouse_Id
        self.Local_Clickhouse_password = Local_Clickhouse_password
        self.Local_Clickhouse_Ip = Local_Clickhouse_Ip
        self.DB_NAME = DB_NAME
        self.TABLE_NAME = TABLE_NAME
        self.connect_db()
        print("ADVER_CATE_INFO Function", self.Extract_Adver_Cate_Info())
        print("Mobon_Com_Code Function", self.Extract_Mobon_Com_Code())
        print("MEDIA_Property_info Function", self.Extract_Media_Property_Info())
        print("Product_property_info Function ", self.Extract_Product_Property_Info())
        print("Function List :")
        print("1. Show_Inner_Df_List()")
        print("2. Extract_Sample_Log()")
        print("3. Extract_Click_View_Log(start_dttm, last_dttm, Adver_Info = False,\
                                Media_Info = False, Product_Info = False, data_size = 1000000)")

    def connect_db(self):
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
                        ADVER_ID,
                        CTGR_SEQ_3,
                        CTGR_NM_3,
                        CTGR_SEQ_2,
                        CTGR_NM_2,
                        CTGR_SEQ_1,
                        CTGR_NM_1
                    FROM
                    TEST.ADVER_PROPERTY_INFO
                 """
            Adver_Cate_Df_sql = text(Adver_Cate_Df_sql)
            self.Adver_Cate_Df = pd.read_sql(Adver_Cate_Df_sql, self.Local_Click_House_Conn)
            return True
        except:
            print("Extract_Adver_Cate_Info error happend")
            return False

    def Extract_Media_Property_Info(self):
        self.connect_db()
        try:
            Media_Property_sql = """
                select
                   MEDIA_SCRIPT_NO,
                   MEDIASITE_NO,
                   MEDIA_ID,
                   SCRIPT_TP_CODE,
                   MEDIA_SIZE_CODE,
                   ENDING_TYPE,
                   M_BACON_YN,
                   ADVRTS_STLE_TP_CODE,
                   MEDIA_CATE_INFO,
                   MEDIA_CATE_NAME
                from
                    TEST.MEDIA_PROPERTY_INFO
                 """
            Media_Property_sql = text(Media_Property_sql)
            self.Media_Property_Df = pd.read_sql(Media_Property_sql, self.Local_Click_House_Conn)
            return self.Extract_Width_Height_Info()
        except Exception as e:
            print("{0} error happend".format(e))
            return False

    def Extract_Width_Height_Info(self):
        df = self.Mobon_Com_Code_Df
        size_mapping_df = df[(df['CODE_TP_ID'] == 'MEDIA_SIZE_CODE') & (~df['CODE_ID'].isin(['99', '16']))][
            ['CODE_ID', 'CODE_VAL']]
        size_mapping_df['WIDTH'] = size_mapping_df['CODE_VAL'].apply(lambda x: x.split('_')[0]).astype('int')
        size_mapping_df['HEIGHT'] = size_mapping_df['CODE_VAL'].apply(lambda x: x.split('_')[1]).astype('int')
        size_mapping_df['SIZE_RATIO'] = size_mapping_df['WIDTH'] / size_mapping_df['HEIGHT']
        size_mapping_df.rename(columns={'CODE_ID': 'MEDIA_SIZE_CODE'}, inplace=True)
        merged_df = pd.merge(self.Media_Property_Df,
                             size_mapping_df[['MEDIA_SIZE_CODE', 'WIDTH', 'HEIGHT', 'SIZE_RATIO']],
                             on=['MEDIA_SIZE_CODE'])
        self.Media_Property_Df = merged_df
        return True

    def Extract_Product_Property_Info(self):
        self.connect_db()
        try:
            PRODUCT_PROPERTY_INFO_sql = """
                select
                    ADVER_ID,
                   PCODE,
                   PRODUCT_CATE_NO,
                   FIRST_CATE,
                   SECOND_CATE,
                   THIRD_CATE,
                   PNM,
                   PRICE
                from
                TEST.SHOP_PROPERTY_INFO
            """
            sql_text = text(PRODUCT_PROPERTY_INFO_sql)
            self.Product_Property_Df = pd.read_sql(sql_text, self.Local_Click_House_Conn)
            return True
        except:
            return False

    def Extract_Mobon_Com_Code(self):
        self.connect_db()
        try:
            MOBON_COM_CODE_sql = """
                select
                    *
                from
                TEST.MOBON_COM_CODE
            """
            sql_text = text(MOBON_COM_CODE_sql)
            self.Mobon_Com_Code_Df = pd.read_sql(sql_text, self.Local_Click_House_Conn)
            return True
        except:
            return False

    def Show_Inner_Df_List(self):
        return ['Adver_Cate_Df', 'Product_Property_Df', 'Media_Property_Df', 'Mobon_Com_Code_Df']

    def Extract_Sample_Log(self, table_name, start_dttm, last_dttm, targeting='AD', sample_cnt=10000):
        data_per_date = int(sample_cnt / 7)
        result_df_list = []
        for WEEK in range(1, 8):
            print(WEEK, "completed")
            random_number = random.random()
            if targeting == 'ALL':
                sample_sql = """
                select 
                LOG_DTTM,
                STATS_DTTM,
                STATS_HH,
                STATS_MINUTE,
                MEDIA_SCRIPT_NO,
                SITE_CODE, 
                ADVER_ID,
                extract(REMOTE_IP,'[0-9]+.[0-9]+.[0-9]+.[0-9]+') AS REMOTE_IP,
                ADVRTS_PRDT_CODE,
                ADVRTS_TP_CODE,
                PLTFOM_TP_CODE,
                PCODE,
                PNAME,
                BROWSER_CODE,
                FREQLOG,
                T_TIME,
                KWRD_SEQ,
                GENDER,
                AGE,
                OS_CODE,
                FRAME_COMBI_KEY,
                CLICK_YN
                FROM TEST.{0}
                sample 0.1
                where 1=1 
                and STATS_DTTM >= {1}
                and STATS_DTTM <= {2}
                and toDayOfWeek(LOG_DTTM) = {0}
                and ADVRTS_PRDT_CODE = '01'
                and RANDOM_SAMPLE > {3}
                and RANDOM_SAMPLE < {4}
                limit {5}
                """.format(table_name, start_dttm, last_dttm, WEEK, random_number, random_number + 0.05, data_per_date)
            else:
                sample_sql = """
                select 
                LOG_DTTM,
                STATS_DTTM,
                STATS_HH,
                STATS_MINUTE,
                MEDIA_SCRIPT_NO,
                SITE_CODE, 
                ADVER_ID,
                extract(REMOTE_IP,'[0-9]+.[0-9]+.[0-9]+.[0-9]+') AS REMOTE_IP,
                ADVRTS_PRDT_CODE,
                ADVRTS_TP_CODE,
                PLTFOM_TP_CODE,
                PCODE,
                PNAME,
                BROWSER_CODE,
                FREQLOG,
                T_TIME,
                KWRD_SEQ,
                GENDER,
                AGE,
                OS_CODE,
                FRAME_COMBI_KEY,
                CLICK_YN
                FROM TEST.{0}
                sample 0.1
                where 1=1
                and STATS_DTTM >= {1}
                and STATS_DTTM >= {2}
                and toDayOfWeek(LOG_DTTM) = {3}
                and ADVRTS_PRDT_CODE = '01'
                and ADVRTS_TP_CODE = '{4}'
                and RANDOM_SAMPLE > {5}
                and RANDOM_SAMPLE < {6}
                limit {7}
                """.format(table_name, start_dttm, last_dttm, WEEK, targeting, random_number, random_number + 0.05,
                           data_per_date)
            sample_sql = text(sample_sql)
            sql_result = pd.read_sql(sample_sql, self.Local_Click_House_Conn)
            result_df_list.append(sql_result)
        result_df = pd.concat(result_df_list)
        return result_df

    def Extract_Click_View_Log(self, start_dttm, last_dttm, Adver_Info=False,
                               Media_Info=False, Product_Info=False, data_size=1000000):
        self.connect_db()
        dt_index = pd.date_range(start=str(start_dttm), end=str(last_dttm))
        dt_list = dt_index.strftime("%Y%m%d").tolist()
        dt_cnt = len(dt_list)
        data_per_dt = int(data_size / dt_cnt)

        return_df_list = []
        data_cnt = 0
        rotation_cnt = int(data_size / 10000)
        while data_cnt < data_size:
            sampling_parameter = random.random()
            minute_sample_parameter = random.randint(0, 60)
            Extract_Data_sql = """
            SELECT * FROM
            {0}
            sample {1}
            where STATS_DTTM = {2}
            and STATS_MINUTE = {3}
            limit {4}
            """.format(self.DB_NAME + '.' + self.TABLE_NAME, sampling_parameter, start_dttm, minute_sample_parameter,
                       data_size)
            sql_text = text(Extract_Data_sql)
            sql_result = pd.read_sql(sql_text, self.Local_Click_House_Conn)
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

    def check_table_name(self, table_name):
        self.connect_db()
        check_table_name_sql = """
            SHOW TABLES FROM {0}
        """.format(self.DB_NAME)
        sql_text = text(check_table_name_sql)
        sql_result = list(pd.read_sql(sql_text, self.Local_Click_House_Conn)['name'])
        print(sql_result)
        if table_name in sql_result:
            return True
        else:
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
    log_data = test_context.Extract_Sample_Log()
    print(log_data)
    print(test_context.Product_Property_Df)
    print(test_context.Adver_Cate_Df)
    print(test_context.Media_Property_Df)
    print(test_context.Mobon_Com_Code_Df)