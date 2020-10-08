from sqlalchemy import create_engine, text
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy import inspect
from clickhouse_sqlalchemy import Table, engines

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
        self.connect_local_db()
        metadata = MetaData(bind=self.Local_Click_House_Engine)
        log_table = Table(table_name, metadata,
                Column('STATS_DTTM', Integer),
                Column('STATS_HH', Integer),
                Column('MINUTE',Integer),
                Column('MEDIA_SCRIPT_NO',String),
                Column('ADVRTS_TP_CODE',String, nullable = False),
                Column('ADVRTS_PRDT_CODE',String, nullable = False),
                Column('PLTFOM_TP_CODE',String, nullable = False),
                Column('SITE_CODE',String),
                Column('ADVER_ID',String),
                Column('PCODE',String, nullable = False),
                Column('PNAME',String, nullable = False),
                Column('REMOTE_IP',String),
                Column('BROWSER_CODE',String, nullable = False),
                Column('FREQLOG',String, nullable = False),
                Column('T_TIME',String, nullable = False),
                Column('KWRD_SEQ',String, nullable = False),
                Column('GENDER',String, nullable = False),
                Column('AGE',String, nullable = False),
                Column('OS_CODE',String, nullable = False),
                Column('CLICK_YN',Integer),
                engines.Memory())
        log_table.create()
        return True
        
        
    def check_table_name(self,table_name ) : 
        self.connect_local_db()
        check_table_name_sql = """
            SHOW TABLES FROM {0}
        """.format(self.DB_NAME)
        sql_text = text(check_table_name_sql)
        sql_result = list(pd.read_sql(sql_text, self.Local_Click_House_Conn))
        if table_name in sql_result :
            return True
        else :
            return False
                
if __name__ == "__main__" :
    test_context = Local_Click_House_DB_Context("click_house_test1","0000","TEST")
    print(test_context.create_table("TEST_2"))
    print(test_context.check_table_name("TEST_2"))
   
