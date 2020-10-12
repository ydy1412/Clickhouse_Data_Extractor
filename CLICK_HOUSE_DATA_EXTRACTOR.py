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


class Click_House_Data_Extractor :

    def __init__( self, clickhouse_id, clickhouse_password, maria_id, maria_password, local_clickhouse_id, local_clickhouse_password,
                  local_clickhouse_db_name, log_name, log_file ):
        self.clickhouse_id = clickhouse_id
        self.clickhouse_password = clickhouse_password
        self.maria_id = maria_id
        self.maria_password = maria_password
        self.local_clickhouse_id = local_clickhouse_id
        self.local_clickhouse_password = local_clickhouse_password

        self.local_clickhouse_db_name = local_clickhouse_db_name

        self.logger = Logger(log_name, log_file)

        self.Click_House_Engine = None
        self.Click_House_Conn = None

        self.MariaDB_Engine = None
        self.MariaDB_Engine_Conn = None

        self.logger.log("connect_db", self.connect_db())
        self.logger.log("Extract_Adver_Cate_info function ", self.Extract_Adver_Cate_Info())
        self.logger.log("Extract_Media_Property_Info function " , self.Extract_Media_Property_Info())

    def connect_db(self) :
        self.Click_House_Engine = create_engine('clickhouse://{0}:{1}@192.168.3.230:8123/'
                                                'MOBON_ANALYSIS'.format(self.clickhouse_id, self.clickhouse_password))
        self.Click_House_Conn = self.Click_House_Engine.connect()
        self.MariaDB_Engine = create_engine('mysql+pymysql://{0}:{1}@192.168.100.108:3306/dreamsearch'
                                            .format(self.maria_id, self.maria_password))
        self.MariaDB_Engine_Conn = self.MariaDB_Engine.connect()
        return True

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

    def Extract_Media_Script_List(self, stats_dttm_hh) :
        self.connect_db()
        try :
            Media_Script_No_Dict = {'01': None, '02': None}
            for PLTFOM_TP_CODE in Media_Script_No_Dict.keys():
                media_script_cnt_sql = """
                SELECT
                    count(*) as cnt 
                    FROM
                    (SELECT distinct media_script_no
                    FROM BILLING.MOB_MEDIA_SCRIPT_HH_STATS
                    WHERE PLTFOM_TP_CODE = '{0}'
                    AND advrts_prdt_code = '01'
                    AND ITL_TP_CODE = '01'
                    AND STATS_DTTM = {1}
                    AND STATS_HH = '{2}'
                    AND TOT_EPRS_CNT > CLICK_CNT) as ms_tb;
                """.format(PLTFOM_TP_CODE, str(stats_dttm_hh)[:-2], str(stats_dttm_hh)[-2:])
                media_script_cnt = pd.read_sql(media_script_cnt_sql, self.MariaDB_Engine_Conn)
                top_10_cnt = int(media_script_cnt.iloc[0].values[0] / 10)

                Ms_List_Sql = """
                select 
                    click_stats_tb.MEDIA_SCRIPT_NO, 
                    click_stats_tb.CLICK_CNT
                    from
                    (SELECT MEDIA_SCRIPT_NO, 
                    sum(TOT_EPRS_CNT) as TOT_EPRS_CNT, 
                    sum(CLICK_CNT) as CLICK_CNT  
                    FROM BILLING.MOB_MEDIA_SCRIPT_HH_STATS
                    where PLTFOM_TP_CODE = '{0}'
                    and advrts_prdt_code = '01'
                    and ITL_TP_CODE = '01'
                    and STATS_DTTM = {1}
                    and STATS_HH = '{2}'
                    and TOT_EPRS_CNT > CLICK_CNT
                    group by MEDIA_SCRIPT_NO) as click_stats_tb
                    order by click_stats_tb.CLICK_CNT desc
                    limit {3};
                """.format(PLTFOM_TP_CODE,str(stats_dttm_hh)[:-2], str(stats_dttm_hh)[-2:], top_10_cnt)
                Ms_List = pd.read_sql(Ms_List_Sql, self.MariaDB_Engine_Conn)['MEDIA_SCRIPT_NO']
                Media_Script_No_Dict[PLTFOM_TP_CODE] = Ms_List
            self.Media_Script_No_Dict = Media_Script_No_Dict
            return True
        except :
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
        for PLTFOM_TP_CODE, Media_Script_List in self.Media_Script_No_Dict.items():
            Merged_Df_List = []
            Media_Script_List_Shape = Media_Script_List.shape[0]
            i = 1
            Total_Data_Cnt = 0
            for MEDIA_SCRIPT_NO in Media_Script_List:
                print("{0}/{1} start".format(i, Media_Script_List_Shape))
                i += 1
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
            
        Concated_Df = pd.concat(Merged_Df_List)
        Concated_Df['CLICK_YN'] = Concated_Df['KOREA_DATE'].apply(lambda x: 0 if pd.isnull(x) else 1)
        if Concated_Df.shape[0] <= Sample_Size:
            final_df = Concated_Df.drop(columns=['KOREA_DATE'])
        else:
            final_df = Concated_Df.drop(columns=['KOREA_DATE']).sample(Sample_Size)
        self.connect_local_db()
        final_df.to_sql(table_name,con = self.Local_Click_House_Engine, index=False, if_exists='append')
        print(final_df.head())
        print(final_df.shape)
        print(final_df.columns)
        print(final_df.info())
        return True

if __name__ == "__main__":

    logger_name = input("logger name is : ")
    logger_file = input("logger file name is : ")
    clickhouse_id = input("click house id : ")
    clickhouse_password = input("clickhouse password : ")
    maria_id = input("maria id : ")
    maria_password = input("maria password : ")
    local_clickhouse_id = input("local clickhouse id : " ) 
    local_clickhouse_password = input("local clickhouse password : " ) 
    local_clickhouse_DB_name = input("local clickhouse DB name : " )
    
    stats_dttm = input("extract dttm is (ex) 20200801 : ) " ) 
    data_cnt_per_hour = input("the number of data to extract per hour : " )
    sample_size = input("Sampling size : " ) 
    
    
    """
    test용 property data
    logger_name = "test"
    logger_file = "test.json"
    clickhouse_id = "analysis"
    clickhouse_password = "analysis@2020"
    maria_id = "analysis"
    maria_password = "analysis@2020"
    local_clickhouse_id = "click_house_test1"
    local_clickhouse_password = "0000"
    local_clickhouse_DB_name = "TEST"
    """

    click_house_context = Click_House_Data_Extractor(clickhouse_id, clickhouse_password,
                                                     maria_id, maria_password,
                                                     local_clickhouse_id,local_clickhouse_password,
                                                     local_clickhouse_DB_name,
                                                     logger_name, logger_file)

    # click_house_context.create_local_table('TEST_5')
    # automatic extracting logic start

    # automatic extracting logic end

    # manual extracting logic

    # manual extracting logic endedt 
    batch_date = datetime.datetime.now()
    date_delta = timedelta(days=10)
    extract_date = batch_date - date_delta
    extract_date = extract_date.strftime('%Y%m%d')
    extract_date_list = [extract_date +'0{0}'.format(i) if i < 10 else extract_date + str(i) for i in range(0,24) ]
    click_house_context.Extract_Date_Range_From_DB()
    test_date = "2020092201"
    extract_click_df_result = click_house_context.Extract_Click_Df(test_date)
    extract_view_df_result = click_house_context.Extract_View_Df(test_date,'TEST_5',5000)

    print(click_house_context.maria_initial_date)
    print(click_house_context.maria_last_date)
    # Adver_Cate_Df = Extract_Adver_Cate_Info()
    # Media_Property_Df = Extract_Media_Property_Info()
    # for extract_date in extract_date_list :
    #     Click_Df = Extract_Click_Df(extract_date)
    #     print("CLick_Df extracted")
    #     Media_Sciprt_List_Dict = Extract_Media_Script_List(extract_date)
    #     print("Media_Sciprt_List_Dict extracted")
    #     Final_View_Df = Extract_View_Df(extract_date,Media_Sciprt_List_Dict,Click_Df,Adver_Cate_Df,Media_Property_Df)
    #     print(Final_View_Df)
    #     break
