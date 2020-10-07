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
        self.Local_Click_House_Engine = create_engine('clickhouse://default@localhost/{0}'.format(self.DB_NAME))
        self.Local_Click_House_Conn = self.Local_Click_House_Engine.connect()
        sql = """
        SHOW TABLES from {0}
        """.format(self.DB_NAME)
        sql_text = text(sql)
        self.sql_result = pd.read_sql_query(sql_text, self.Local_Click_House_Conn)


if __name__ == "__main__" :
    test_context = Local_Click_House_DB_Context("test","test","test")
    print(test_context.sql_result)
