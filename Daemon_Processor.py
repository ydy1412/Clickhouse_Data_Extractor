import os
import sys
from dateutil.tz import tzlocal
from datetime import datetime
import time

import argparse

from CLICK_HOUSE_DATA_EXTRACTOR import Click_House_Data_Extractor
from logger import Logger

def run_data_extractor(stats_dttm, logger_name, log_file, args) :
    logger_name = logger_name
    log_file = log_file
    error_logger = Logger("error","/logger_forder/{0}_error_log_file.json".format(stats_dttm))
    click_house_context = Click_House_Data_Extractor(args.clickhouse_db_id,
                                                     args.clickhouse_db_password,
                                                     args.test_db_id,
                                                     args.test_db_password,logger_name, log_file)
    property_extractor_test = False
    if click_house_context.connect_db() :
        if click_house_context.Extract_Adver_Cate_Info() :
            if click_house_context.Extract_Media_Property_Info() :
                property_extractor_test = True

    if property_extractor_test == False :
        error_logger.log("property extractor test" , 'Failed!')
        return False

    extract_date_list = [stats_dttm +'0{0}'.format(i) if i < 10 else stats_dttm + str(i) for i in range(0,24) ]

    ## for testing
    # for extract_date in extract_date_list[0]:

    for extract_date in extract_date_list :

        if click_house_context.Extract_Media_Script_List(extract_date) == False :
            error_logger.log("Extract_Media_Script_List function", 'Failed!')
            return False

        if click_house_context.Extract_Click_Df(extract_date) == False :
            error_logger.log("Extract_Click_Df function", 'Failed!')
            return False

        if click_house_context.Extract_View_Df(extract_date) == False :
            error_logger.log("Extract_View_Df function", 'Failed!')
            return False

if __name__ == "__main__" :
    parser = argparse.ArgumentParser()
    parser.add_argument("--test_db_id",help="test db id", default=None)
    parser.add_argument("--test_db_password", help="test db password", default=None)
    parser.add_argument("--clickhouse_db_id", help="clickhouse db id", default=None)
    parser.add_argument("--clickhouse_db_password", help="clickhouse db password", default=None)
    args = parser.parse_args()

    while True :
        extractor_finished = False
        local_tz = tzlocal()
        now_time = datetime.now(tz=local_tz)
        now_time_str = datetime.now(tz=local_tz).strftime("%Y%m%d%H")
        Year_month_value = now_time_str[:6]
        Date = now_time_str[6:8]
        Hour = now_time_str[8:10]
        time.sleep(300)
