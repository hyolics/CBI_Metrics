import numpy as np
import pandas as pd
from datetime import datetime
from collections import Counter, defaultdict
import uuid
from psycopg2 import connect
import pymysql
from pymongo import MongoClient
from bson.binary import JAVA_LEGACY
from bson.codec_options import CodecOptions
from bson.objectid import ObjectId
# define by myself
from DBConnection import *
from FilterInner import *
from UserData import *
from Update2Sheet import *

dbConfig = Configure('DB_Configuration.json').configures
dbConf = DBConfig['UserBehaviourAtMySQL']

auth_mapping = {0: 'Password',
                1: 'AD',
                2: 'Facebook',
                3: 'Weibo',
                4: 'QQ',
                5: 'NTHU',
                6: 'GooglePLUS',
                7: 'Twitter',
                8: 'LinkedIn',
                9: 'Wechat',
                10: 'Clearstone'}

class CBIData(UserBehaviorData):
    def member_info(self, Query, mode=0):
        '''overwrite: get member table from AWS'''
        conn = connect(host=dbConf['ip'], db=dbConf['db'], user=dbConf['username'], passwd=dbConf['passwd'])
        df = pd.read_sql(Query, conn)

        df['register_date'] = df['register_date_time'].map(lambda x: x.strftime('%Y-%m-%d'))
        try:
            df['auth_type'] = df['auth_type'].map(auth_mapping)
        except:
            pass

        '''paste member IP from MySQL and apply FilterInner.py'''
        id_list = [str(x) for x in df['id']]
        conn = connect(host=dbConf['ip'], db=dbConf['db'], user=dbConf['username'], passwd=dbConf['passwd'])
        Query = "SELECT member_id, message, created_date_time FROM access_log where member_id in ('"
        Query += "','".join(df['id'])
        Query += "') and access_code = 'Login' and result = 'successful' and created_date_time < '{0}' ".format(self.endDate)
        df2 = pd.read_sql(Query, conn)

        df2 = df2.groupby(['member_id']).agg({'message': lambda x: Counter(x).most_common(1)[0][0], 'created_date_time': 'last'}).reset_index()
        df2['message'].fillna(-1, inplace=True)
        df2['inner'] = df2['message'].map(lambda x: Filter_IP(x))

        df = pd.merge(df, df2, left_on='id', right_on='member_id')
        df = Filter_Email(df)
        df.drop('member_id', axis=1, inplace=True)
        if mode == 1:
            return df[df['inner'] == 0].reset_index()
        else:
            return df
            
    def privilege(self, Query):
        '''overwrite: get member privilege from MYSQL'''
        conn = pymysql.connect(host=dbConf['ip'], db=dbConf['db'], user=dbConf['username'], passwd=dbConf['passwd'])
        df = pd.read_sql(Query, conn)
        return df

    def paid_status(self, id_list):
        '''get current paid status from AWS'''
        type_mapping = {1: 'DS', 2: 'QI', 3: 'PV', 4: 'PS', 5: 'DD', 7: 'SEP'}
        status_mapping = {0: '', 1: 'inactive', 2: 'active', 3: 'active', 4: 'backend', 
                10: 'paid', 12: 'paid', 13: 'paid'}
        conn = connect(host=dbConf['ip'], db=dbConf['db'], user=dbConf['username'], passwd=dbConf['passwd'])
        Query = "SELECT * FROM dbo.product_trial where member_id in('"
        Query += "','".join(id_list)
        Query += "')"
        df = pd.read_sql(Query, conn)
        df['type'] = df['type'].map(type_mapping)
        df['status'] = df['status'].map(status_mapping)
        df = df.pivot(index='member_id', columns='type', values='status').reset_index()
        return product

    def CBI_privilege(self, product_code, id_list=None):
        '''get privilege infomation and preprocess'''
        Query = "SELECT member_id, product_code, auth_start_date, auth_end_date FROM model_privilege where "
        Query += "product_code in('"
        Query += "','".join(product_code)
        Query += "')"

        if id_list != None:
            Query += "and member_id in('"
            Query += "','".join(id_list)
            Query += "')"

        df = self.privilege(Query)
        df.drop_duplicates(subset='member_id', keep='last', inplace=True)
        df = df.groupby('member_id').agg({'auth_start_date': 'last',
                    'auth_end_date': 'last'}).reset_index()
        df['auth_start_date'] = df['auth_start_date'].map(
            lambda x: datetime.strptime(str(x)[:10], '%Y-%m-%d'))
        df['auth_end_date'] = df['auth_end_date'].map(
            lambda x: datetime.strptime(str(x)[:10], '%Y-%m-%d') if x != None else None) 
        return df

    def CBI_member(self, id_list, privilege):
        '''針對給定的id_list抓member資訊，並與privilege合併'''
        source_mapping = {'backend': 'Backstage',
                'backstage': 'Backstage',
                'docketNavigator': 'Docket Navigator',
                'frontend': 'Frontend',
                'import': 'Frontend',
                'import.foxconn': 'Frontend',
                'main': 'Frontend'}
        col =  ['id', 'Account', 'Registration Date', 'Registration Source', 'Auth Type',
                'Company', 'Job Title', 'Industry Classification', 'Corporate', 'inner',
                'member_id', 'Trial Status']

        if len(id_list)>0:
            Query = "SELECT id, account, source, register_date_time, email_domain, auth_type, company_name, industrial_type_key, title, corp_id FROM dbo.member where id in('"
            Query += "','".join(id_list)
            Query += "')"
            df = self.member_info(Query, mode=0)
            df['source'] = df['source'].map(source_mapping)
            df['source'].fillna('frontend', inplace=True)

            df['register_date_time'] = df['register_date_time'].map(lambda x: str(x)[:11])
            df = df[['id', 'account', 'register_date_time', 'source', 'auth_type',
                            'company_name', 'title', 'industrial_type_key', 'corp_id', 'inner']]
            df.columns = col[:-2]

            privilege = pd.merge(privilege, df, how='outer', left_on='member_id', right_on='id')
            privilege['member_id'].fillna(privilege['id'], inplace=True)
            
            now = datetime.today()
            privilege.loc[privilege['auth_start_date'].isna(), 'Trial Status'] = 'Inact_Trial'
            privilege.loc[privilege['auth_start_date'].notna(), 'Trial Status'] = 'Act_Trial'
            privilege.loc[(privilege['auth_end_date'].notna())&(privilege['auth_end_date'] <= now), 'Trial Status'] = 'Expired'
        else:
            privilege = pd.DataFrame(columns=col)
        return privilege

    def insert_sheet(self, df):
        '''upload CBI data to google sheet'''
        if len(df)>0:
            # Insert sheet
            spreadsheet_id = '1MSvQBVzg3R3YBWR2Bj3GcqSdFM_BOG7oyt6w-Eumtvc'
            ## insert
            update_user(spreadsheet_id, df)
        else:
            print('No data!')

    def insert_data(self, Query, df):
        pymysql.converters.encoders[np.int64] = pymysql.converters.escape_int
        pymysql.converters.conversions = pymysql.converters.encoders.copy()
        pymysql.converters.conversions.update(pymysql.converters.decoders)
        if len(df)>0:
            conn = connect(host=dbConf['ip'], db=dbConf['db'], user=dbConf['username'], passwd=dbConf['passwd'])
            for i in range(len(df)):
                tmp = df.iloc[i,].values.tolist()
                cursor = conn.cursor()
                cursor.execute(Query, tmp)
                conn.commit()
        else:
            print('No data!')
