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
    def __init__(self, Local_CLickhouse_Id, Local_Clickhouse_password, DB_NAME):
        self.Local_Clickhouse_Id = Local_CLickhouse_Id
        self.Local_Clickhouse_password = Local_Clickhouse_password
        self.DB_NAME = DB_NAME
        self.connect_local_db()

    def connect_local_db(self) :
        self.Local_Click_House_Engine = create_engine('clickhouse://{0}:{1}@localhost/{2}'.format(self.Local_Clickhouse_Id, self.Local_Clickhouse_password, self.DB_NAME))
        self.Local_Click_House_Conn = self.Local_Click_House_Engine.connect()
        return True

    def create_table(self, table_name) :
        client = Client(host='localhost')
        DDL_sql = """
        CREATE TABLE IF NOT EXISTS {0}.{1}
        (
            STATS_DTTM  UInt32,
            STATS_HH  UInt8,
            STATS_MINUTE UInt8, 
            MEDIA_SCRIPT_NO String,
            SITE_CODE String,
            ADVER_ID String,
            REMOTE_IP String,
            ADVRTS_PRDT_CODE Nullable(String),
            ADVRTS_TP_CODE Nullable(String),
            PLTFOM_TP_CODE Nullable(String),
            PCODE Nullable(String),
            PNAME Nullable(String), 
            BROWSER_CODE Nullable(String),
            FREQLOG Nullable(String),
            T_TIME Nullable(String),
            KWRD_SEQ Nullable(String),
            GENDER Nullable(String),
            AGE Nullable(String),
            OS_CODE Nullable(String),
            CLICK_YN UInt8,
            BATCH_DTTM DateTime
        ) ENGINE = MergeTree
        PARTITION BY  STATS_DTTM
        ORDER BY (STATS_DTTM, STATS_HH)
        SAMPLE BY STATS_DTTM
        TTL BATCH_DTTM + INTERVAL 90 DAY
        SETTINGS index_granularity=8192
        """.format(self.DB_NAME, table_name) 
        result = client.execute(DDL_sql)
        return result
        
    def check_table_name(self,table_name ) : 
        self.connect_local_db()
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
    test_context = Local_Click_House_DB_Context("click_house_test1","0000","TEST")
    # print(test_context.create_table("TEST_2"))
    # print(test_context.create_table_2("TEST_3"))
    print(test_context.check_table_name("TEST_3"))
   
