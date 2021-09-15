'''SEP CBI'''
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, date
from collections import Counter, defaultdict
import itertools 
import uuid
from pandas.io.json import json_normalize
import warnings
warnings.filterwarnings('ignore')
from GetData import *
from Preprocessing import *
from Update2Sheet import *

# tab define
tab_new = {'DeclarationStatus': 'ETSI', 
           'CompanyProfile': 'Company Profile Detail', 
           'Watchlist': 'Watchlist',
           'ClaimChart': 'Claim Summary'}    
tab_list = ['ETSI', 'Company Profile List', 'Company Profile Detail',
            'Watchlist', 'Claim Summary', 'TSR', 'CSS']

# index define
CBI_list = ['ETSI CBI', 'Company Profile List CBI',
           'Company Profile Detail CBI',
           'Watchlist CBI', 'Claim Summary CBI',
           'TSR CBI', 'CSS CBI']
column_list = ['ETSI CBI', 'ETSI Time', 'ETSI Filter Submit #',
               'ETSI Export List #', 'ETSI Click Patent Link #',
               'Company Profile List CBI', 'Company Profile List Time',
               'Company Profile Detail CBI', 'Company Profile Detail Time',
               'Company Profile List Filter Submit #',
               'Company Profile Export List #',
               'Company Profile Click Patent Link #',
               'Company Profile Checked Company',
               'Watchlist CBI', 'Watchlist Time', 'Watchlist # Link Copied',
               'Claim Summary CBI', 'Claim Summary Time',
               'Claim Summary Checked SEPs', 'Claim Summary Download TS',
               'TSR CBI', 'TSR Time', 'TSR View #',
               'CSS CBI', 'CSS Time', 'CSS View #']

#require
col_name = 'userId'
criterion = 10
filters = {'SEP_DeclarationStatus_AdvancedFilter_Save',
           'SEP_DeclarationStatus_AdvancedFilter_Submit',
           'SEP_CompanyProfile_AdvancedFilter_Save', 
           'SEP_CompanyProfile_AdvancedFilter_Submit'}
watchlist = {'SEP_Watchlist_Declaration_Share'}

def uilog_save(x):
    return len(x[(x=='SEP_DeclarationStatus_AdvancedFilter_Save')|
                (x=='SEP_CompanyProfile_AdvancedFilter_Save')])
def uilog_keyword(x):
    return len(x[x=='SEP_ClaimChart_Keyword'])
def uilog_link(x):
    return len(x[x=='SEP_Watchlist_Declaration_Share'])
def uilog_list(uilog_list):
    return len([x for x in uilog_list if x=='List001_open'])
def uilog_filter(uilog_list):
    return len([x for x in uilog_list 
                if 'Filter_Submit' in x or 'Filter_Save' in x])
def uilog_export(uilog_list):
    return len([x for x in uilog_list if x=='SEP_Export'])
def referer_list(referer_list):
    return len(set([x.split('patid=')[1] for x in referer_list 
                    if x.count('pd=SEP') and x.count('patid=')]))

def click_check(uilog_list, require_uilog):
    return pd.Series(uilog_list).str.contains('|'.join(require_uilog)).sum()
def revisit_tab(x):
    return len(x[x!='-'])

def get_SEPs(df):
    df['SEPs'] = None
    df['patentNo'] = None
    df['Checked SEPs'] = None
    filter_cond = (df['tab']=='Claim Summary')|(df['tab']=='TSR')|(df['tab']=='CSS')
    for index, row in df[filter_cond][['referer']].iterrows():
        tmp = row['referer']
        if tmp.count('?declaration='):
            df.loc[index, 'SEPs'] = (tmp.split('?declaration=')[1]).split('&')[0]
        elif tmp.count('?decNo='):
            df.loc[index, 'SEPs'] = (tmp.split('?decNo=')[1]).split('&')[0]
        else:
            df.loc[index, 'SEPs'] = None

        if tmp.count('&patentNo='):
            df.loc[index, 'patentNo'] = tmp.split('&patentNo=')[1]
        elif tmp.count('&patId='):
            df.loc[index, 'patentNo'] = (tmp.split('&patId=')[1]).split('&')[0]
        else:
            df.loc[index, 'patentNo'] = None

    for index, row in df[filter_cond][['SEPs', 'patentNo']].iterrows():
        if (row['patentNo']!=None)&(row['SEPs']!=None):
            df.loc[index, 'Checked SEPs'] = str(row['patentNo']) + ' | ' + str(row['SEPs'])
    return df  

def revisit_check(id_list):
    if len(id_list)>0:
        conn = pymysql.connect(host='10.60.90.219',
                                   user='julia', passwd='julia', db='dbo')
        Query = '''select * from CBI_SEP
                where userId in (' '''
        Query += "','".join(id_list)
        Query += "')"
        df = pd.read_sql(Query, conn)
        df = df.groupby('userId').agg({'O_CBI': revisit_tab,
                                      'CPL_CBI': revisit_tab,
                                      'CPD_CBI': revisit_tab,
                                      'W_CBI': revisit_tab,
                                      'CS_CBI': revisit_tab,
                                      'TSR_CBI': revisit_tab,
                                      'CSS_CBI': revisit_tab,}).reset_index()
        df.columns = ['userId', *tab_list]
    else:
        df = pd.DataFrame()
    return df

def scroll_80(input_df, output_df):
    #df: raw uilog, output_df: tab_based
    output_df['scroll'] = 'N'
    for index, row in input_df[input_df['uilogCode'].str.contains('_Scroll')].iterrows():
        if (row['tab']==tab_list[0])&(row['uilogValue']=='3GPP Tech Bodies'):
            output_df.loc[(output_df['userId']==row['userId'])&\
                         (output_df['tab']==tab_list[0]), 'scroll'] = 'Y'
        elif (row['tab']==tab_list[1])&\
            ((row['uilogValue']=='3GPP Tech Bodies')|(row['uilogValue']=='Remaining Life')):
            output_df.loc[(output_df['userId']==row['userId'])&\
                         (output_df['tab']==tab_list[1]), 'scroll'] = 'Y'
        else:
            pass
    return output_df

def company_click(input_df, output_df):
    #input_df: raw uilog, output_df: member_based
    col = 'Company Profile Checked Company'
    tmp = input_df[input_df['uilogCode']=='SEP_CompanyProfile_List']
    if len(tmp)>0:
        tmp = tmp.groupby('userId').agg({'uilogValue': lambda x: list(set(x))}).reset_index()
        tmp.columns = ['userId', col]
        tmp['Checked Company #'] = tmp[col].map(len)
        tmp[col] = tmp[col].map(str)
        tmp[col] = [(x.split('[')[1]).split(']')[0] for x in tmp[col]]
        tmp[col] = [x.replace("'", '') for x in tmp[col]]      
        output_df = pd.merge(output_df, tmp, how='left', on='userId')
        output_df['Checked Company #'].fillna(0, inplace=True)
        output_df[col].fillna('-', inplace=True)
    else:
        output_df['Checked Company #'] = 0
        output_df[col] = '-'
    return output_df

def download_TS(input_df, output_df):
    #input_df: raw uilog, output_df: member_based
    col = 'Claim Summary Download TS'
    tmp = input_df[input_df['uilogCode']=='SEP_ClaimChart_Download']
    if len(tmp)>0:
        tmp = tmp.groupby('userId').agg({'uilogValue': lambda x: list(set(x))}).reset_index()
        tmp.columns = ['userId', 'Download TS']
        tmp['Download TS'] = tmp['Download TS'].map(str)
        tmp['Download TS'] = [(x.split('[')[1]).split(']')[0] for x in tmp['Download TS']]
        tmp[col] = [x.replace("'", '') for x in tmp['Download TS']]      
        output_df = pd.merge(output_df, tmp, how='left', on='userId')
        output_df['Download TS'].fillna('-', inplace=True)
        output_df[col].fillna('-', inplace=True)
    else:
        output_df['Download TS'] = '-'
        output_df[col] = '-'
    return output_df

def golden_feature(df, df2):
    #df:member_based, df2: tab_based
    for index, row in df[['userId', 'Download TS', 'keyword', 'link']].iterrows():
        output_list =  list()
        if row['Download TS']!='-':
            output_list.append('Download TS')
        if row['keyword']>0:
            output_list.append('Check Keyword Expansion')
        if row['link']>0:
            output_list.append('Copy Share Link')
        
        for index2, row2 in df2[df2['userId']==row['userId']].iterrows():
            if (row2['tab']==tab_list[0])&(row2['filter']>0):
                output_list.append('Filter Submit in ETSI')
            if (row2['tab']==tab_list[0])&(row2['export']>0):
                output_list.append('Export List to PV/CSV in ETSI')
            if (row2['tab']==tab_list[0])&(row2['list']>0):
                output_list.append('Click on Patent Link in ETSI')
            if (row2['tab']==tab_list[1])&(row2['filter']>0):
                output_list.append('Filter Submit in Company Profile')
            if (row2['tab']==tab_list[1])&(row2['export']>0):
                output_list.append('Export List to PV/CSV in Company Profile')
            if (row2['tab']==tab_list[1])&(row2['list']>0):
                output_list.append('Click on Patent Link in Company Profile')
            
        df.loc[index, 'Golden Feature'] = str(sorted(output_list))
    df['Golden Feature'] = [(x.split('[')[1]).split(']')[0] for x in df['Golden Feature']]
    df['Golden Feature'] = [x.replace("'", '') for x in df['Golden Feature']]      
    return df

def CBI_tab(df_input, df, tab, mode, pattern=None, other_col='Total Time (Tab)', crit=0):
    #df_input: tab_based
    id_list = [x for x in set(df_input['userId'])]
    revisit = revisit_check(id_list)
    for index, row in df_input.iterrows():
        filter_cond = df[col_name]==row['userId']
        if row['tab'] == tab:
            df.loc[filter_cond, tab + ' CBI'] = 'N'
            df.loc[filter_cond, tab + ' Time'] = row['Avg. Time (Tab)']
            if (row['tab']==tab_list[-1]) | (row['tab']==tab_list[-2]):
                df.loc[filter_cond, tab + ' View #'] = row['View #']
            if (row['tab']==tab_list[0]) | (row['tab']==tab_list[2]):
                df.loc[filter_cond, tab + ' Filter Submit #'] = row['filter']
                df.loc[filter_cond, tab + ' Export List #'] = row['export']
                df.loc[filter_cond, tab + ' Click Patent Link #'] = row['list']
            try:
                tmp = revisit[revisit['userId']==row['userId']][tab].values[0]
            except:
                tmp = 0
            if row['Total Time (Tab)'] > criterion:
                if (mode==0)|(tmp>0):
                    df.loc[filter_cond, tab + ' CBI'] = 'Y'
                elif (mode==1)&((row['scroll']=='Y')|
                (click_check(row['uilogCode'], pattern)>=1)|(tmp>0)):
                    df.loc[filter_cond, tab + ' CBI'] = 'Y'
                elif (mode==2)&((row[other_col]>crit)|
                (click_check(row['uilogCode'], pattern)>=1)|(tmp>0)):
                    df.loc[filter_cond, tab + ' CBI'] = 'Y'
                else:
                    pass
    return df

class SEPMetrics(object):
    def __init__(self, df):
        self.df = df
        
    def uilog_format(self):
        self.df = json_normalize(self.df, ['uilog'], ['userId', 'visitorId', 'sessionId', 'referer', 'ip', 'country'])
        self.df = uilog_format(self.df)
        self.df['inner'] = self.df['ip'].map(lambda x: Filter_IP(x))
        self.df = self.df[self.df['inner'] == 0]
        self.df = uilog_product(self.df)
        self.df = uilog_stay(self.df)
        self.df = self.df[self.df['uilogCode'].notna()]
        self.df = self.df[self.df['referer'].str.contains('https://app.patentcloud.com')]
        self.df.loc[(self.df['detail']=='patentInfo')&
            (self.df['referer'].str.contains('pd=SEP')), 'product'] = 'SEP'
        self.df = self.df[self.df['product']=='SEP'].reset_index()
        return self.df
    
    def uilog_preprocessing(self):
        ''' uilog preprocess'''
        ## fillna visitorId by sessionId
        self.df.loc[self.df['visitorId'].isna(), 'sessionNo'] = self.df[self.df['visitorId'].isna()].groupby(['sessionId']).ngroup()
        self.df.loc[self.df['visitorId'].isna(), 'visitorId'] = self.df['sessionNo'].map(
                lambda x: 'visitor_' + str(x).split('.')[0])
        ## fillna userId by visitorId
        self.df['userId'] = self.df.groupby(['visitorId'])['userId'].fillna(method='bfill')
        self.df['userId'] = self.df.groupby(['visitorId'])['userId'].fillna(method='ffill')
        self.df.loc[self.df['userId'].isna(), 'visitorNo'] = self.df[self.df['userId'].isna()].groupby(['visitorId']).ngroup()
        self.df.loc[self.df['userId'].isna(), 'userId'] = self.df['visitorNo'].map(
                lambda x: 'visitor_' + str(x).split('.')[0])
        del self.df['sessionNo']
        del self.df['visitorNo']
        
        self.df = uilog_tab(self.df)
        self.df.loc[(self.df['uilogCode']=='freeTrial')|
                (self.df['uilogCode']=='subscribe'), 'tab'] = 'Pricing Page'
        self.df.loc[(self.df['detail']=='patentInfo')&
            (self.df['referer'].str.contains('pd=SEP')), 
                    'uilogCode'] = 'List001_open'
        filter_NA = self.df['tab'].isna()
        self.df.loc[filter_NA, 'tab'] = 'ETSI'
        
        self.df = get_SEPs(self.df)
        return self.df
    
    def member_based(self, merge_df):
        '''row by users'''
        #df: uilog, merge_self: privilege
        if len(self.df)>0:
            output = self.df.groupby(['userId', 'sessionId']).agg(
                {'visitorId': 'last', 'country':'last',
                 'stay': sum, 'uilogTime': max, 
                 'uilogCode': [uilog_save, uilog_link, uilog_keyword],
                 'tab': [lambda x: len(set(x)), list],
                 'Checked SEPs': lambda x: list(set(x[x!=None]))}).reset_index()
            output.columns = ['userId', 'sessionId', 'visitorId', 'Country',
                           'Total Time', 'Date', 'save', 'link', 'keyword',
                            '# of Total Tab Views', 'Tab Visited',
                            'Claim Summary Checked SEPs']
            output = output.groupby(['userId']).agg(
                {'visitorId': 'last', 'Country':'last', 'Total Time': sum, 
                 'Date':max, 'save':sum, 'link': sum, 'keyword': sum,
                 '# of Total Tab Views': sum, 'Tab Visited':sum,
                 'Claim Summary Checked SEPs': sum}).reset_index()
            
            output['Total Time'] = output['Total Time'].map(lambda x: x.seconds)
            output['Total Time'] = output['Total Time'].map(lambda x: sec2hour(x))
            output['Update Date'] = output['Date'].map(lambda x: str(x)[:11])
            output = company_click(self.df, output)
            output = download_TS(self.df, output)
            output['Claim Summary Checked SEPs'] = output['Claim Summary Checked SEPs'].map(
                                                        lambda x: [xx for xx in x 
                                                                   if str(xx) != 'nan' and xx!=None])
            output['Claim Summary Checked SEPs'] = [','.join(map(str, x)) for x in 
                                                         output['Claim Summary Checked SEPs']]
            output['Tab Visited'] = output['Tab Visited'].map(lambda x: [xx for xx in x if xx != None])
            output['Tab Visited'] = output['Tab Visited'].map(lambda x: sorted(list(set(x))))
            output['Tab Visited'] = [','.join(map(str, x)) for x in output['Tab Visited']]
            output['Traffic Source'] = 'Patentcloud'
            
            if len(merge_df) > 0:
                output = pd.merge(output, merge_df, how='left',
                                left_on='userId', right_on='member_id')
            else:
                col_list = ['product_code', 'auth_start_date',
                    'auth_end_date', 'id', 'Account', 'Registration Date',
                    'Registration Source', 'Auth Type', 'Company', 'Job Title',
                    'Industry Classification', 'Corporate', 'inner', 'Trial Status']
                for col in col_list:
                    if col not in output.columns:
                        output[col] = '-'
        return output

    def tab_based(self, merge_df):
        '''row by tabs'''
        #self.df: uilog, merge.df: member_based
        if len(self.df) > 0:
            output = self.df.groupby(['userId', 'tab']).agg(
                {'sessionId': lambda x: len(set(x)), 
                'referer': lambda x: list(itertools.chain(x)),
                'uilogCode': lambda x: list(itertools.chain(x)), 
                 'stay': sum, 'SEPs': lambda x: list(set(x[x!=None]))}).reset_index()
            output.columns = ['userId', 'tab', 'Page Views', 'referer',
                             'uilogCode', 'Total Time (Tab)', 'View #']
            
            output['Total Time (Tab)'] = output['Total Time (Tab)'].map(lambda x: x.seconds)
            output['Avg. Time (Tab)'] = output['Total Time (Tab)'] / output['Page Views']
            output['Avg. Time (Tab)'] = output['Total Time (Tab)'].map(lambda x: sec2hour(x))
            output['View #'] = output['View #'].map(lambda x: len(set([xx for xx in x 
                                                                       if str(xx)!='nan' and xx!=None])))
            output['filter'] = output['uilogCode'].map(lambda x: uilog_filter(x))
            output['export'] = output['uilogCode'].map(lambda x: uilog_export(x))
            output['list'] = output['referer'].map(lambda x: referer_list(x))
            output = scroll_80(self.df, output)
            output = pd.merge(merge_df, output, on='userId')
        else:
            pass
        return output
    
    def CBI(self, raw, output):
        #raw: tab-based, output: member-based
        # init.
        id_list = [x for x in set(raw['userId'])]
        for col in column_list:
            if col not in output.columns:
                output[col] = '-'
        output.loc[:, 'Tier'] = ''
        output.loc[:, 'Identity'] = ''
        output.loc[:, 'CSD'] = ''
        output.loc[:, 'Group'] = ''
        
        # Identity
        output.loc[~output['userId'].str.contains('visitor'), 'Identity'] = 'MQL'
        #other
        output = golden_feature(output, raw)
    
        for index, element in enumerate(id_list):
            tmp = raw[raw[col_name]==element]
            #ETSI Overview
            output = CBI_tab(tmp, output, tab_list[0], mode=1, pattern=filters)
            #Company Profile
            output = CBI_tab(tmp, output, tab_list[1], mode=2, pattern=filters, 
                             other_col='Checked Company #', crit=1)
            output = CBI_tab(tmp, output, tab_list[2], mode=2, pattern=filters,
                            other_col='Checked Company #')
            #Watchlist
            output = CBI_tab(tmp, output, tab_list[3], mode=2, pattern=watchlist,
                            other_col='save')
            #Claim Chart
            output = CBI_tab(tmp, output, tab_list[4], mode=0)
            output = CBI_tab(tmp, output, tab_list[5], mode=0)
            output = CBI_tab(tmp, output, tab_list[6], mode=0)
        return output

    def PQL(self, output):
        # check overall CBI   
        output['Y'] = output.loc[:,CBI_list].apply(lambda x: Counter(x).get('Y'), axis=1)
        output['Y'].fillna(0, inplace=True)

        #identify PQL
        output.loc[(output['Identity']=='MQL')&(output['Y']>=2), 'PQL'] = 'Y'
        output['PQL'].fillna('N', inplace=True)
        return output
    
    def output_format(self, output):
        #rename
        output.rename(columns={'userId': 'Member ID',
                          'visitorId': 'Visitor ID'}, inplace=True)
        
        # final tune
        output = output[output['inner']!=1].reset_index()
        output.fillna('-', inplace=True)
        output.loc[output['Member ID'].str.contains('visitor'), 'Member ID'] = '-'
        output.loc[output['Visitor ID'].str.contains('visitor'), 'Visitor ID'] = '-'
        output = output[output['Visitor ID']!='-']

        # sort
        output = output[['PQL', 'Visitor ID', 'Member ID', 
                 'Tier', 'Identity', 'CSD', 'Group', 
                 'Registration Source', 'Traffic Source',
                 'Job Title', 'Industry Classification',
                 'Registration Date', 'Trial Status', 
                 'Country', 'Company', 'Corporate',
                 'Update Date', '# of Total Tab Views', 'Total Time',
                'Tab Visited', 'Golden Feature',
                *column_list]]
        output.sort_values(by=['PQL', 'Identity'], ascending=[False, False], inplace=True)
        output = output.reset_index(drop=True)
        return output

if __name__ == '__main__':
    # start = '2021-08-08 00:00:00'
    # end = '2021-08-09 00:00:00'
    # startDate = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    # endDate = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
    # now = str(endDate + timedelta(days=1))[:10] + ' 00:00:00'
    now = datetime.today()

    start = str(now - timedelta(days=1, hours=16))[:19]
    end = str(now - timedelta(days=0, hours=16))[:19]
    startDate = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    endDate = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
    now = datetime.strptime(str(now)[:10], '%Y-%m-%d')

    Query = '''insert into CBI_SEP (PQL, visitorId, userId,
        traffic_source, trial_status, created_date, tab_num,
        total_time, tab_visited, golden_feature,
        O_CBI, O_time, O_filter, O_export, O_link,
        CPL_CBI, CPL_time, CPD_CBI, CPD_time, 
        CP_filter, CP_export, CP_link, CP_company, 
        W_CBI, W_time, W_copied, 
        CS_CBI, CS_time, CS_sep, CS_download,
        TSR_CBI, TSR_time, TSR_view, CSS_CBI, CSS_time, CSS_view)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s)'''

    a = CBIData(start, end)
    a.format_date(pattern='%Y-%m-%d %H:%M:%S')
    pipeline = [{'$match': {'startTime': {'$gte': startDate, '$lte': endDate}}},
                {'$project': {'_id': 0, 'userId.id': 1, 
                            'visitorId': '$visitorId', 'sessionId': 1, 
                            'country': '$client.country',
                            'referer': '$request.header.Referer',
                            'startTime': '$startTime', 
                            'uilog': '$log.uiLog', 'ip':'$client.realip'}}]
    uilog = a.UiLog(pipeline)

    if len(uilog)>0:
        SEP = SEPMetrics(uilog)
        uilog = SEP.uilog_format()
        if len(uilog)>0:
            uilog = SEP.uilog_preprocessing()

            id_list = [str(x) for x in set(uilog[uilog['product']=='SEP']['userId']) if 'visitor' not in x]
            privilege = a.CBI_Privilege(['Lic_SEP_FREE_TRIAL'], id_list)
            privilege = a.CBI_Member(id_list, privilege)
            member_df = SEP.member_based(privilege)
            tab_df = SEP.tab_based(member_df)
            output = SEP.CBI(tab_df, member_df)
            output = SEP.PQL(output)
            output = SEP.output_format(output)

            drop_index = ['Tier', 'Identity', 'CSD', 'Group',
                    'Registration Source', 'Job Title',
                    'Industry Classification', 'Registration Date',
                    'Country', 'Company', 'Corporate']
            sheet_index = ['Member ID', 'Tier', 'Identity', 'CSD', 'Group']
            output1 = output.loc[output['Member ID']!='-', sheet_index]
            output1.drop_duplicates('Member ID', inplace=True)
            output2 = output.drop(columns=drop_index)

            a.CBI_upload(output1)
            a.CBI_DB(Query, output2)
    print(str(start)+' finish!')
