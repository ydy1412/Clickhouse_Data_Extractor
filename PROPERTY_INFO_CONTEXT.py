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


class PROPERTY_INFO_CONTEXT :
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

    def connect_db(self):
        self.MariaDB_Engine = create_engine('mysql+pymysql://{0}:{1}@192.168.100.106:3306/dreamsearch'
                                            .format(self.Maria_id, self.Maria_password))
        self.MariaDB_Engine_Conn = self.MariaDB_Engine.connect()

        self.Local_Click_House_Engine = create_engine(
            'clickhouse://{0}:{1}@localhost/{2}'.format(self.Clickhouse_id, self.Clickhouse_password,
                                                        self.Clickhouse_DB))
        self.Local_Click_House_Conn = self.Local_Click_House_Engine.connect()

        return True

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

    def create_shop_property_table(self, ADVER_ID_LIST, table_name):
        self.connect_db()
        product_property_df_list = []
        size = ADVER_ID_LIST.shape[0]
        data_count = 0
        now_time = datetime.now(tz=tzlocal()).strftime("%Y%m%d%H")
        print("Start_time :", now_time)
        for i, ADVER_ID in enumerate(ADVER_ID_LIST):
            price_info_sql = """
             SELECT 
             USERID as ADVER_ID,
             PCODE,PNM,
             PRICE
             FROM dreamsearch.SHOP_DATA
             WHERE USERID = '{0}';
             """.format(ADVER_ID)
            sql_text = text(price_info_sql)
            try:
                product_price_info_df = pd.read_sql(sql_text, self.MariaDB_Engine_Conn)
                merged_df = pd.merge(self.product_cate_info_df, product_price_info_df, on=['ADVER_ID', 'PCODE'])
                product_property_df_list.append(merged_df)
            except:
                self.connect_db()
                product_price_info_df = pd.read_sql(sql_text, self.MariaDB_Engine_Conn)
                merged_df = pd.merge(self.product_cate_info_df, product_price_info_df, on=['ADVER_ID', 'PCODE'])
                product_property_df_list.append(merged_df)
            if i % 10 == 0:
                print("{0}/{1} : ".format(i, size), ADVER_ID)
        now_time = datetime.now(tz=tzlocal()).strftime("%Y%m%d%H")
        final_df = pd.concat(product_property_df_list)
        print("End time : ", now_time)
        final_df.to_sql(table_name, con=self.Local_Click_House_Engine, index_label='id', if_exists='replace')
        return True

    def create_adver_property_table(self,table_name) :
        self.connect_db()
        try:
            Adver_Cate_Df_sql = """
                            select
                                MCUI.USER_ID as ADVER_ID, ctgr_info.* 
                                from  dreamsearch.MOB_CTGR_USER_INFO as MCUI
                                left join
                                (
                                SELECT 
                                third_depth.CTGR_SEQ_NEW as CTGR_SEQ_3, third_depth.CTGR_NM as CTGR_NM_3,
                                second_depth.CTGR_SEQ_NEW as CTGR_SEQ_2, second_depth.CTGR_NM as CTGR_NM_2,
                                first_depth.CTGR_SEQ_NEW as CTGR_SEQ_1, first_depth.CTGR_NM as CTGR_NM_1
                                from dreamsearch.MOB_CTGR_INFO third_depth
                                join dreamsearch.MOB_CTGR_INFO second_depth
                                join dreamsearch.MOB_CTGR_INFO first_depth
                                on 1=1 
                                AND third_depth.CTGR_DEPT = 3
                                AND second_depth.CTGR_DEPT = 2
                                AND first_depth.CTGR_DEPT = 1
                                AND second_depth.USER_TP_CODE = '01'
                                AND second_depth.USER_TP_CODE = first_depth.USER_TP_CODE
                                AND third_depth.USER_TP_CODE = second_depth.USER_TP_CODE
                                AND second_depth.HIRNK_CTGR_SEQ = first_depth.CTGR_SEQ_NEW
                                AND third_depth.HIRNK_CTGR_SEQ = second_depth.CTGR_SEQ_NEW) as ctgr_info
                                on MCUI.CTGR_SEQ = ctgr_info.CTGR_SEQ_3;
                         """
            Adver_Cate_Df = pd.read_sql(Adver_Cate_Df_sql, self.MariaDB_Engine_Conn)
            Adver_Cate_Df = Adver_Cate_Df.drop_duplicates(subset='ADVER_ID')
            Adver_Cate_Df.to_sql(table_name,con = self.Local_Click_House_Engine, index_label='id',if_exists='replace')
            return True
        except:
            print("Extract_Adver_Cate_Info error happend")
            return False

    def create_media_property_table(self,table_name ):
        self.connect_db()
        try :
            PAR_PROPERTY_INFO_sql = """
            select 
                ms.no as MEDIA_SCRIPT_NO,
                MEDIASITE_NO,
                ms.userid as MEDIA_ID,
                mpi.SCRIPT_TP_CODE,
                mpi.MEDIA_SIZE_CODE,
                product_type as "ENDING_TYPE",
                m_bacon_yn as "M_BACON_YN",
                ADVRTS_STLE_TP_CODE as "ADVRTS_STLE_TP_CODE",
                media_cate_info.scate as "MEDIA_CATE_INFO",
                media_cate_info.ctgr_nm as "MEDIA_CATE_NAME"
                from dreamsearch.media_script as ms
                join
                (
                SELECT no, userid, scate, ctgr_nm
                FROM dreamsearch.media_site as ms
                join
                (SELECT mpci.CTGR_SEQ, CTGR_SORT_NO, mci.CTGR_NM
                FROM dreamsearch.MEDIA_PAR_CTGR_INFO as mpci
                join dreamsearch.MOB_CTGR_INFO as mci
                on mpci.CTGR_SEQ = mci.CTGR_SEQ_NEW) as media_ctgr_info
                on ms.scate = media_ctgr_info.CTGR_SORT_NO) as media_cate_info
                join
                (select PAR_SEQ, ADVRTS_PRDT_CODE,SCRIPT_TP_CODE, MEDIA_SIZE_CODE 
                from dreamsearch.MEDIA_PAR_INFO
                where PAR_EVLT_TP_CODE ='04') as mpi
                on ms.mediasite_no = media_cate_info.no
                and media_cate_info.scate = {0}
                and mpi.par_seq = ms.no;
            """
            result_list = []
            for i in range(1, 18):
                result = pd.read_sql(PAR_PROPERTY_INFO_sql.format(i), self.MariaDB_Engine_Conn)
                result_list.append(result)
            Media_Info_Df = pd.concat(result_list)
            Media_Info_Df['MEDIA_SCRIPT_NO'] = Media_Info_Df['MEDIA_SCRIPT_NO'].astype('str')
            Media_Info_Df.to_sql(table_name,con = self.Local_Click_House_Engine, index_label='id',if_exists='replace')
            return True
        except :
            return False

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
