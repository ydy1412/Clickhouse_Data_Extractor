# 설치가 필요한 파이썬 라이브러리 정보.
# pip install clickhouse-driver==0.1.3
# pip install clickhouse-sqlalchemy==0.1.4
# conda install sqlalchemy==1.3.16
# pip install ipython-sql==0.4.0

from sqlalchemy import create_engine, text
import pandas as pd
import re
from datetime import date
import datetime
import calendar
from datetime import date
from datetime import timedelta, timezone
from logger import Logger
from clickhouse_sqlalchemy import Table, engines
from clickhouse_driver import Client
import argparse



class Click_House_Data_Extractor :

    def __init__( self, clickhouse_id, clickhouse_password, maria_id, maria_password, local_clickhouse_id,
                  local_clickhouse_password, local_clickhouse_db_name, logger_name = "test", logger_file="inner_logger.json",Maria_DB = False ):

        self.clickhouse_id = clickhouse_id
        self.clickhouse_password = clickhouse_password

        self.maria_id = maria_id
        self.maria_password = maria_password

        self.local_clickhouse_id = local_clickhouse_id
        self.local_clickhouse_password = local_clickhouse_password
        self.local_clickhouse_db_name = local_clickhouse_db_name
        self.logger = Logger(logger_name, logger_file)

        self.Click_House_Engine = None
        self.Click_House_Conn = None

        self.MariaDB_Engine = None
        self.MariaDB_Engine_Conn = None

        self.Maria_DB = Maria_DB

        self.connect_db()

    def connect_db(self) :
        self.Click_House_Engine = create_engine('clickhouse://{0}:{1}@192.168.3.230:8123/'
                                                'MOBON_ANALYSIS'.format(self.clickhouse_id, self.clickhouse_password))
        self.Click_House_Conn = self.Click_House_Engine.connect()
        
        if self.Maria_DB : 
            self.MariaDB_Engine = create_engine('mysql+pymysql://{0}:{1}@192.168.100.108:3306/dreamsearch'
                                                .format(self.maria_id, self.maria_password))
            self.MariaDB_Engine_Conn = self.MariaDB_Engine.connect()
        return True
    def connect_local_db(self):
        self.Local_Click_House_Engine = create_engine(
            'clickhouse://{0}:{1}@localhost/{2}'.format(self.local_clickhouse_id, self.local_clickhouse_password,
                                                        self.local_clickhouse_db_name))
        self.Local_Click_House_Conn = self.Local_Click_House_Engine.connect()
        return True

    def create_local_table(self, table_name):
        client = Client(host='localhost')
        DDL_sql = """
        CREATE TABLE IF NOT EXISTS {0}.{1}
        (
            LOG_DTTM DateTime('Asia/Seoul'),
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
            FRAME_COMBI_KEY Nullable(String),
            CLICK_YN UInt8,
            BATCH_DTTM DateTime
        ) ENGINE = MergeTree
        PARTITION BY  STATS_DTTM
        ORDER BY (STATS_DTTM, STATS_HH)
        SAMPLE BY STATS_DTTM
        TTL BATCH_DTTM + INTERVAL 90 DAY
        SETTINGS index_granularity=8192
        """.format(self.local_clickhouse_db_name, table_name)
        result = client.execute(DDL_sql)
        return result

    def create_entire_log_table(self, table_name):
        client = Client(host='localhost')
        DDL_sql = """
                CREATE TABLE IF NOT EXISTS {0}.{1}
                (
                    mediaId       String,
                    inventoryId   String,
                    frameId       String,
                    logType       String,
                    adType        String,
                    adProduct     String,
                    adCampain     String,
                    adverId       String,
                    productCode   String,
                    cpoint        Decimal(13, 2),
                    mpoint        Decimal(13, 2),
                    auid          String,
                    remoteIp      String,
                    platform      String,
                    device        String,
                    browser       String,
                    createdDate   DateTime default now(),
                    freqLog       Nullable(String),
                    tTime         Nullable(String),
                    kno           Nullable(String),
                    kwrdSeq       Nullable(String),
                    gender        Nullable(String),
                    age           Nullable(String),
                    osCode        Nullable(String),
                    price         Nullable(Decimal(13, 2)),
                    frameCombiKey Nullable(String)
                )  engine = MergeTree() 
                PARTITION BY toYYYYMMDD(createdDate)
                PRIMARY KEY (mediaId, inventoryId, adverId) 
                ORDER BY (mediaId, inventoryId, adverId) 
                SAMPLE BY mediaId 
                TTL createdDate + INTERVAL 90 DAY
                SETTINGS index_granularity = 8192
                """.format(self.local_clickhouse_db_name, table_name)
        result = client.execute(DDL_sql)
        return result

    def check_local_table_name(self, table_name):
        self.connect_local_db()
        check_table_name_sql = """
            SHOW TABLES FROM {0}
        """.format(self.local_clickhouse_db_name)
        sql_text = text(check_table_name_sql)
        sql_result = list(pd.read_sql(sql_text, self.Local_Click_House_Conn)['name'])
        print(sql_result)
        if table_name in sql_result:
            return True
        else:
            return False

    def Extract_Adver_Cate_Info(self) :
        self.connect_db()
        try :
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
        except :
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
        self.MariaDB_Shop_Engine = create_engine('mysql+pymysql://{0}:{1}@192.168.100.106:3306/dreamsearch'
                                            .format(self.maria_id, self.maria_password))
        self.MariaDB_Shop_Conn = self.MariaDB_Shop_Engine.connect()
        try :
            Extract_AdverID_sql = """
            SELECT 
                apci.ADVER_ID,
                apci.PRODUCT_CODE as PCODE,
                apci.ADVER_CATE_NO as PRODUCT_CATE_CODE,
                apsc.FIRST_CATE, 
                apsc.SECOND_CATE, 
                apsc.THIRD_CATE
                FROM
                dreamsearch.ADVER_PRDT_CATE_INFO as apci
            join
                (select * 
                from dreamsearch.ADVER_PRDT_STANDARD_CATE) as apsc
            on apci.ADVER_CATE_NO = apsc.no;
            """
            sql_text = text(Extract_AdverID_sql)
            result = pd.read_sql(sql_text,self.MariaDB_Shop_Conn)
            print(result.head())
            ADVER_ID_LIST = result['ADVER_ID'].unique()
            number_of_ADVER_ID = len(ADVER_ID_LIST)
            print(number_of_ADVER_ID)
            print(int(number_of_ADVER_ID/10))
            divide_cnt = int(number_of_ADVER_ID/10)
            Price_Info_List = []
            i = 0

            for ADVER_ID in ADVER_ID_LIST :
                i += 1
                if i % divide_cnt == 0 :
                    print(i)
                Price_Info_sql = """
                SELECT
                USERID as ADVER_ID,
                PCODE,
                PRICE
                FROM
                dreamsearch.SHOP_DATA
                WHERE
                USERID = '{0}';
                """.format(ADVER_ID)
                sql_text = text(Price_Info_sql)
                Price_Info_List.append(pd.read_sql(sql_text,self.MariaDB_Shop_Conn))
            Price_Info_df = pd.concat(Price_Info_List)
            Product_Info_df = pd.merge(result, Price_Info_df, on=['ADVER_ID','PCODE'], how='left')
            print(Product_Info_df)
            # Product_Info_df.to_sql('PRODUCT_PROPERTY_INFO', con=self.Local_Click_House_Engine, index=False )
            return True
        except :
            return False

    def Extract_Click_Stats_Date(self, stats_dttm_hh, hours=1):
        str_stats_dttm = str(stats_dttm_hh)
        stats_date = datetime.datetime(int(str_stats_dttm[:4]), int(str_stats_dttm[4:6]), int(str_stats_dttm[6:8]),
                                       int(str_stats_dttm[8:]))
        hour_delta = timedelta(hours=hours)
        previus_date = stats_date + hour_delta
        previus_date = previus_date.strftime('%Y%m%d%H')
        return int(stats_dttm_hh), int(previus_date)

    def Extract_Click_Df(self, stats_dttm_hh) :
        self.connect_db()
        try :
            Click_Date_List = [self.Extract_Click_Stats_Date(stats_dttm_hh,1)[0],self.Extract_Click_Stats_Date(stats_dttm_hh,1)[1]]
            Click_Data_Df_List = []
            for Click_Date_Key in Click_Date_List:
                Click_Df_sql = """
                select toTimeZone(createdDate, 'Asia/Seoul')            as KOREA_DATE,
                              inventoryId as MEDIA_SCRIPT_NO,
                              adCampain as SITE_CODE,
                              remoteIp as REMOTE_IP
                       from MOBON_ANALYSIS.MEDIA_CLICKVIEW_LOG
                       where 1 = 1
                         and inventoryId <> ''
                         and adCampain <> ''
                         and remoteIp <> ''
                         and logType = 'C'
                         and toYYYYMMDD(createdDate) = {0}
                         and toHour(createdDate) = {1}
                """.format(str(Click_Date_Key)[:-2], str(Click_Date_Key)[-2:])
                Click_Df_sql = text(Click_Df_sql)
                Click_Df = pd.read_sql_query(Click_Df_sql, self.Click_House_Conn)
                Click_Data_Df_List.append(Click_Df)
            self.Click_Df = pd.concat(Click_Data_Df_List)
            self.logger.log("Extract_click_Df_{0}".format(stats_dttm_hh),"success")
            return True
        except :
            return False
    
    def Extract_Date_Range_From_DB(self) : 
        self.connect_db()
        try : 
            maria_db_sql = """
                select min(stats_dttm) as initial_date, max(stats_dttm) as last_date from BILLING.MOB_CAMP_MEDIA_HH_STATS;
            """
            maria_db_sql = text(maria_db_sql)
            result = pd.read_sql(maria_db_sql,self.MariaDB_Engine_Conn)
            self.maria_initial_date = result['initial_date'].values[0]
            self.maria_last_date = result['last_date'].values[0]
        except: 
            pass
        
        try :
            clickhouse_db_sql = """
                select
                min(toYYYYMMDD(createdDate)) as initial_date
                max(toYYYYMMDD(createdDate)) as last_date 
                from MOBON_ANALYSIS.MEDIA_CLICKVIEW_LOG
                where 1=1
                limit 10;
            """
            clickhouse_db_sql = text(clickhouse_db_sql)
            result = pd.read_sql(clickhouse_db_sql, self.Click_House_Conn)
            self.clickhouse_initial_date = result['initial_date'].values[0]
            self.clickhouse_last_date = result['last_date'].values[0]
        except: 
            pass
        return True

    def Extract_Media_Script_List(self, stats_dttm_hh, min_click_cnt = 5) :
        self.connect_db()
        Ms_List_Sql = """
        SELECT
        tb.inventoryId as MEDIA_SCRIPT_NO, tb.cnt
        from
        (SELECT
        inventoryId, count(*) as cnt
            FROM MOBON_ANALYSIS.MEDIA_CLICKVIEW_LOG
               where 1 = 1
                 and inventoryId <> ''
                 and adCampain <> ''
                 and remoteIp <> ''
                 and logType = 'C'
                 and toYYYYMMDD(createdDate) = {0}
                 and toHour(createdDate) = {1}
        group by inventoryId
        order by  count() desc) as tb
        where tb.cnt >= {2}
        """.format(str(stats_dttm_hh)[:-2], str(stats_dttm_hh)[-2:], min_click_cnt)
        try :
            Ms_List_Sql = text(Ms_List_Sql)
            Ms_result = pd.read_sql_query(Ms_List_Sql, self.Click_House_Conn)
            self.Ms_List = Ms_result['MEDIA_SCRIPT_NO']
            self.logger.log("Extract_Media_Script_list function","Success")
            return True
        except :
            self.logger.log("Extract_Media_Script_list function", "Failed")
            return False

    def Extract_View_Df(self,
                        stats_dttm_hh,
                        table_name,
                        Maximum_Data_Size = 2000000,
                        Sample_Size = 500000) :
        
        Media_Script_No_Dict = self.Extract_Media_Script_List(stats_dttm_hh)
        i = 0
        if Media_Script_No_Dict == False :
            while i < 5 :
                i += 1
                Media_Script_No_Dict = self.Extract_Media_Script_List(stats_dttm_hh)
            if Media_Script_No_Dict == False:
                return "Extract_Media_Script_List Function error"
        Media_Script_cnt = self.Ms_List.shape[0]
        i = 0
        Total_Data_Cnt = 0
        Merged_Df_List = []
        for MEDIA_SCRIPT_NO in self.Ms_List :
            i += 1
            # print("{0}/{1} start".format(i, Media_Script_List_Shape))
            View_Df_sql = """
            select 
                createdDate as LOG_DTTM,
                toYYYYMMDD(toTimeZone(createdDate, 'Asia/Seoul') )  as STATS_DTTM,
               toHour(toTimeZone(createdDate, 'Asia/Seoul') ) as STATS_HH,
               toMinute(toTimeZone(createdDate, 'Asia/Seoul') ) as STATS_MINUTE,
                      inventoryId as MEDIA_SCRIPT_NO,
                      adType                                           as ADVRTS_TP_CODE,
                      multiIf(
                              adProduct IN ('mba', 'nor', 'banner', 'mbw'), '01',
                              adProduct IN ('sky', 'mbb', 'sky_m'), '02',
                              adProduct IN ('ico', 'ico_m'), '03',
                              adProduct IN ('scn'), '04',
                              adProduct IN ('nct', 'mct'), '05',
                              adProduct IN ('pnt', 'mnt'), '07',
                              'null'
                          )                                            as ADVRTS_PRDT_CODE,
                      multiIf(
                              platform IN ('web', 'w', 'W'), '01',
                              platform IN ('mobile', 'm', 'M'), '02',
                              'null'
                          )                                            as PLTFOM_TP_CODE,
                      adCampain as SITE_CODE,
                      adverId as ADVER_ID,
                      visitParamExtractRaw(productCode, 'productCode') as PCODE,
                      visitParamExtractRaw(productCode, 'productName') as PNAME,
                      remoteIp as REMOTE_IP,
                      visitParamExtractRaw(browser, 'code')            as BROWSER_CODE,
                      freqLog as FREQLOG,
                      tTime as T_TIME,
                      kwrdSeq as KWRD_SEQ,
                      gender as GENDER,
                      age as AGE,
                      osCode as OS_CODE,
                      frameCombiKey as FRAME_COMBI_KEY,
                    now() as BATCH_DTTM
               from MOBON_ANALYSIS.MEDIA_CLICKVIEW_LOG
               where 1 = 1
                 and inventoryId = '{0}'
                 and adCampain <> ''
                 and remoteIp <> ''
                 and logType = 'V'
                 and toYYYYMMDD(createdDate) = {1}
                 and toHour(createdDate) = {2}
            """.format(MEDIA_SCRIPT_NO, str(stats_dttm_hh)[:-2], str(stats_dttm_hh)[-2:])
            View_Df_sql = text(View_Df_sql)
            try:
                View_Df = pd.read_sql_query(View_Df_sql, self.Click_House_Conn)
                Click_View_Df = pd.merge(View_Df, self.Click_Df, on=['MEDIA_SCRIPT_NO', 'SITE_CODE', 'REMOTE_IP'],
                                         how='left')
                Merged_Df_List.append(Click_View_Df)
            except:
                self.connect_db()
                View_Df = pd.read_sql_query(View_Df_sql, self.Click_House_Conn)
                Click_View_Df = pd.merge(View_Df, self.Click_Df, on=['MEDIA_SCRIPT_NO', 'SITE_CODE', 'REMOTE_IP'],
                                         how='left')
                Merged_Df_List.append(Click_View_Df)
            Total_Data_Cnt += Click_View_Df.shape[0]
            if Total_Data_Cnt >= Maximum_Data_Size :
                break
        try : 
            Concated_Df = pd.concat(Merged_Df_List)
            Concated_Df['CLICK_YN'] = Concated_Df['KOREA_DATE'].apply(lambda x: 0 if pd.isnull(x) else 1)
            if Concated_Df.shape[0] <= Sample_Size:
                final_df = Concated_Df.drop(columns=['KOREA_DATE'])
            else:
                final_df = Concated_Df.drop(columns=['KOREA_DATE']).sample(Sample_Size)
            self.connect_local_db()
            self.logger.log("Extract view log to df","success")
            print(final_df.head())
            print(final_df.shape)
            final_df.to_sql(table_name,con = self.Local_Click_House_Engine, index=False, if_exists='append')
            self.logger.log("Insert Data to local db","success")
            return True
        except :
            self.logger.log("something error happened","error")
            return False

    def Extract_All_Log_Data(self, stats_dttm, table_name):
        self.connect_local_db()
        log_sql = """
        SELECT * 
        FROM 
        MOBON_ANALYSIS.MEDIA_CLICKVIEW_LOG
        where 1 = 1
             and toYYYYMMDD(createdDate) = {0}
        """.format( str(stats_dttm))
        print(log_sql)
        log_sql = text(log_sql)
        try:
            View_Df = pd.read_sql_query(log_sql, self.Click_House_Conn)
            print(View_Df.shape)
            View_Df.to_sql(table_name, con=self.Local_Click_House_Engine, index=False, if_exists='append')
        except:
            self.connect_db()
            self.connect_local_db()
            View_Df = pd.read_sql_query(log_sql, self.Click_House_Conn)
            View_Df.to_sql(table_name, con=self.Local_Click_House_Engine, index=False, if_exists='append')
        self.logger.log("Extract view log to df", "success")
        return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--auto",help="(auto : Y, manual : N, migration : M, Test : T ) ", default='T')
    parser.add_argument("--create_table", help="--add_table ~~ ", default = None )
    args = parser.parse_args()

    # For test
    # logger_name = "test"
    # logger_file = "test.json"
    # clickhouse_id = "analysis"
    # clickhouse_password = "analysis@2020"
    # maria_id = "dyyang"
    # maria_password = "dyyang123!"
    # local_clickhouse_id = "click_house_test1"
    # local_clickhouse_password = "0000"
    # local_clickhouse_DB_name = "TEST"
    # local_table_name = 'TEST10'
    # data_cnt_per_hour = 1000
    # sample_size = 1000

    # for service
    logger_name = input("logger name is : ")
    logger_file = input("logger file name is : ")

    clickhouse_id = input("click house id : ")
    clickhouse_password = input("clickhouse password : ")
    maria_id = input("maria id : ")
    maria_password = input("maria password : ")
    local_clickhouse_id = input("local clickhouse id : " )
    local_clickhouse_password = input("local clickhouse password : " )
    local_clickhouse_DB_name = input("local clickhouse DB name : " )
    local_table_name = input("local cllickhouse table name : " )

    data_cnt_per_hour = input("the number of data to extract per hour : " )
    sample_size = input("Sampling size : " )

    logger = Logger(logger_name, logger_file)
    logger.log("auto mode", args.auto.upper())

    # logger.log("extract start", 'Y')

    # clickhouse data extract context 생성
    click_house_context = Click_House_Data_Extractor(clickhouse_id, clickhouse_password,
                                                     maria_id, maria_password,
                                                     local_clickhouse_id,local_clickhouse_password,
                                                     local_clickhouse_DB_name)

    logger.log("clickhouse context load", "success")
    if args.create_table == 'click_yn' :
        table_name = input("click_yn table name : " ) 
        click_house_context.create_local_table(table_name)
        local_table_name = args.create_table
        logger.log("create clickhouse table {0} success".format(local_table_name), 'True')
    elif args.create_table == 'entire_log_yn' : 
        table_name = input("entire_log_yn table : " ) 
        click_house_context.create_entire_log_table(table_name)
        logger.log("create entire_log_yn table", "True")


    if args.auto.upper() == 'Y' :
        # automatic extracting logic start
        batch_date = datetime.datetime.now()
        click_house_context.Extract_Date_Range_From_DB()
        date_delta = timedelta(days=10)
        extract_date = batch_date - date_delta
        extract_date = extract_date.strftime('%Y%m%d')
        # automatic extracting logic end

    elif args.auto.upper() == 'N' :
        # manual extracting logic start
        start_dttm = input("extract start dttm is (ex) 20200801 ) : " )
        from_hh = input("start hour is (ex) 00 hour : 00 ) : ")
        last_dttm = input("extract last dttm is (ex) 20200827 ) : " )
        dt_list = pd.date_range(start=start_dttm, end=last_dttm).strftime("%Y%m%d").tolist()
        
        for stats_dttm in dt_list :        
            logger.log("manual_stats_dttm",stats_dttm)
            stats_dttm_list = [stats_dttm + '0{0}'.format(i) if i < 10 else stats_dttm + str(i) for i in range(int(from_hh), 24)]
            for Extract_Dttm in stats_dttm_list :
                extract_click_df_result = click_house_context.Extract_Click_Df(Extract_Dttm)
                extract_view_df_result = click_house_context.Extract_View_Df(Extract_Dttm, local_table_name, int(data_cnt_per_hour),
                                                                             int(sample_size))
                logger.log("Manual_extracting {0} result ".format(Extract_Dttm), extract_view_df_result)
            # manual extracting logic end

    if args.auto.upper() == 'M' :
        start_dttm = input("extract start dttm is (ex) 20200801 ) : ")
        from_hh = input("start hour is (ex) 00 hour : 00 ) : ")
        last_dttm = input("extract last dttm is (ex) 20200827 ) : ")
        dt_list = pd.date_range(start=start_dttm, end=last_dttm).strftime("%Y%m%d").tolist()

        for stats_dttm in dt_list:
            logger.log("Migration stats_dttm", stats_dttm)
            migrate_log_df_result = click_house_context.Extract_All_Log_Data(stats_dttm,local_table_name)
            logger.log("Migration {0} result ".format(Extract_Dttm), migrate_log_df_result)

    elif args.auto.upper() == 'T' :
        return_value = click_house_context.Extract_Product_Property_Info()
        print(return_value)
    else :
        pass
