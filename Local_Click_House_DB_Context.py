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

class Local_Click_House_DB_Context :
    def __init__(self, maria_id, maria_password,
                 Local_Clickhouse_Id, Local_Clickhouse_password, Local_Clickhouse_Ip,
                 DB_NAME, TABLE_NAME):

        self.Maria_id = maria_id
        self.Maria_password = maria_password

        self.Local_Clickhouse_Id = Local_Clickhouse_Id
        self.Local_Clickhouse_password = Local_Clickhouse_password
        self.Local_Clickhouse_Ip = Local_Clickhouse_Ip
        self.DB_NAME = DB_NAME
        self.TABLE_NAME = TABLE_NAME
        self.connect_db()
        self.Extract_Adver_Cate_Info()
        self.Extract_Media_Property_Info()
        self.Extract_Product_Property_Info()

    def connect_db(self) :
        self.Local_Click_House_Engine = create_engine('clickhouse://{0}:{1}@{2}/{3}'.format(self.Local_Clickhouse_Id,
                                                                                            self.Local_Clickhouse_password,
                                                                                            self.Local_Clickhouse_Ip,
                                                                                            self.DB_NAME))
        self.Local_Click_House_Conn = self.Local_Click_House_Engine.connect()

        self.MariaDB_Engine = create_engine('mysql+pymysql://{0}:{1}@192.168.100.108:3306/dreamsearch'
                                            .format(self.Maria_id, self.Maria_password))
        self.MariaDB_Engine_Conn = self.MariaDB_Engine.connect()
        return True

    def Extract_Adver_Cate_Info(self):
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
            self.Adver_Cate_Df = Adver_Cate_Df.drop_duplicates(subset='ADVER_ID')
            return True
        except:
            print("Extract_Adver_Cate_Info error happend")
            return False

    def Extract_Media_Property_Info(self) :
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
            self.Media_Info_Df = pd.concat(result_list)
            self.Media_Info_Df['MEDIA_SCRIPT_NO'] = self.Media_Info_Df['MEDIA_SCRIPT_NO'].astype('str')
            return True
        except :
            return False

    def Extract_Product_Property_Info(self):
        self.Product_Info_Df = None
        pass

    def Extract_Click_View_Log (self, stats_dttm, stats_hh = None, Adver_Info = True,
                                Media_Info = True, Product_Info = True,
                                sample_size= 0.1, data_size = 1000000) :
        self.connect_db()
        if stats_hh != None :
            Extract_Data_sql = """
            SELECT * FROM
            {0}
            sample {1}
            where STATS_DTTM = {2}
            and STATS_HH = {3}
            limit {4}
            """.format(self.DB_NAME+'.'+self.TABLE_NAME, sample_size, stats_dttm, stats_hh, data_size )
            sql_text = text(Extract_Data_sql)
            sql_result = pd.read_sql(sql_text, self.Local_Click_House_Conn)
        else :
            print("no stats_hh")
            Extract_Data_sql = """
            SELECT * FROM
            {0}
            sample {1}
            where STATS_DTTM = {2}
            limit {3}
            """.format(self.DB_NAME+'.'+self.TABLE_NAME, sample_size,
                       stats_dttm, data_size )
            print(Extract_Data_sql)
            sql_text = text(Extract_Data_sql)
            sql_result = pd.read_sql(sql_text,self.Local_Click_House_Conn)
        if Adver_Info == True :
            sql_result = pd.merge(sql_result, self.Adver_Cate_Df, on=['ADVER_ID'],how='left')
        if Media_Info == True :
            sql_result = pd.merge(sql_result, self.Media_Info_Df, on=['MEDIA_SCRIPT_NO'],how='left')
        if Product_Info == True :
            sql_result = pd.merge(sql_result, self.Product_Info_Df, on=['PCODE'], how = 'left')
        return sql_result

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
    local_clickhouse_Table_name = 'TEST_7'
    test_context = Local_Click_House_DB_Context(maria_id, maria_password,
                                                local_clickhouse_id,local_clickhouse_password,
                                                local_clickhouse_ip,local_clickhouse_DB_name,
                                                local_clickhouse_Table_name)
    log_data = test_context.Extract_Click_View_Log('20200924',Product_Info=False,sample_size = 0.1, data_size=100000)
    print(test_context.Adver_Cate_Df)
    print(log_data.head())