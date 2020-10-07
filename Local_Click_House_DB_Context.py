from sqlalchemy import create_engine, text
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
        
    def check_table_name(self,table_name ) : 
        self.connect_local_db()
        check_table_name_sql = """
            SHOW TABLES FROM {1}
            WHERE name = {0}
        """.format(table_name, self.DB_NAME)
        sql_text = text(check_table_name_sql)
        sql_result = pd.read_sql(sql_text, self.Local_Click_House_Conn)
        if sql_result.shape[0] == 1:
            return True
        else :
            return False
        
if __name__ == "__main__" :
    test_context = Local_Click_House_DB_Context("click_house_test1","0000","TEST")
    print(test_context.check_table_name("TEST"))
