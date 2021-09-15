'''QI CBI'''
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, date
import re
from collections import Counter, defaultdict
import itertools 
import uuid
from pandas.io.json import json_normalize
import warnings
warnings.filterwarnings('ignore')
import sys
sys.path.append('/home/julia/code/function')
from GetData import *
from Preprocessing import *
from Update2Sheet import *

#tab define
tab_raw = ['quality-insights-summary',
          'quality-insights-history',
          'quality-insights-claim-analysis',
          'quality-insights-claim-insights',
          'quality-insights-family-prior-art', 
          'quality-insights-prior-art-finder',
          'quality-insights-semantic-prior-art',
          'quality-insights-file-wrapper-search']
    
tab_new = ['Overview', 'History', 'Claim Analysis', 'Claim Insights', 
      'Family Prior Art', 'Prior Art Finder', 'Semantic Prior Art', 
      'File Wrapper Search', 'Prior Art Analytics']
tab_mapping = dict()
for index, element in enumerate(tab_raw):
    tab_mapping[element] = tab_new[index]

# index define
iter_list = ['CBI', 'Time', '# of Tab Views']
index_list = list()
for index, element in enumerate(tab_new):
    for index2, element2 in enumerate(iter_list):
        index_list.append(element+' '+element2)

CBI_list = ['Sign Up # of page views', 
        'Download Report #', 'Save Report #',
        'Editable Report # of View', 'KEEP # of Click',
        'KEEP Board # of View', 'KEEP Board # of Records', 
        'Subscribe / Upgrade # of Click', 'Shopping Cart # of View']
CBI_list.insert(1, index_list)
CBI_list = list(list_flatten(CBI_list))
DN_list = ['Unlock Time', 'Unlock # of Views',
        'Unlock # of click PURCHASE', 'Unlock # of click CONTACT']
column_list = [*CBI_list, *DN_list]
channel_list = ['Patentcloud', 'Docket Navigator', 'UnifiedPatents']

# require 
col_name = 'userId'
criterion = 10
overview = {'QI_Summary', 'List001_open'}
history = {'QI_Overview', 'List001_open', 'QI_LandingPage'}
analysis = {'QI_Claim_Analysis_SelectTerms_term'}
insights = {'QI_PriorArt_Comparison_Confirm', 'QI_ClaimElement', 'QI_ClaimElement_Claims', 'QI_ClaimTable_Values'}
family = {'QI_Family_Backward1', 'QI_Family_Backward2'}
finder = {'QI_2ndDegree_Prior_Art2', 'QI_2ndDegree_Prior_Art3'}
wrapper = {'QI_FWS', 'QI_LandingPage', 'List001_open','VI_FWS_ProsecutionOCR', 'VI_CLAIMPDF'}
analytics = {'PriorArt_Analysis_Confirm'}
keep = {'QI_Keep'}
signup = {'freeTrial', 'DN_ENTER_REGISTER_PAGE', 'QI_Freetrial2'}
final = {'subscribe', 'freeTrial', 'QI_Order_Purchased', 'QI_Purchased_Active', 
         'VI_AddCart', 'VI_Simple_AddCart', 'VI_Purchase',
         'QI_Unlock_Data', 'QI_Purchase', 'QI_Discount',
         'QI_Subscribe', 'QI_Pricing'} 

#func.
def uilog_prior(x):
    return len(x[x=='PriorArt_Analysis_Confirm'])
def uilog_keep(x):
    return len(x[x=='QI_Keep_Mode'])
def uilog_download(x):
    return len(x[x=='VI_PrintedVersion'])
def uilog_save(x):
    return len(x[x=='QI_SaveReport'])
def uilog_pricing(x):
    return len(x[(x=='QI_Pricing')|(x=='subscribe')])
def uilog_unlock(x):
    return len(x[x=='QI_Unlock_Data'])
def uilog_contact(x):
    return len(x[x=='Contact0001'])
def uilog_purchase(x):
    return len(x[(x=='QI_Purchase')|(x=='QI_Subscribe')])
def uilog_signup(x):
    return len(x[(x=='freeTrial')|(x=='DN_ENTER_REGISTER_PAGE')|(x=='QI_Freetrial2')])
def ref_project(x):
    return len(x[(x.str.contains('projectId'))&
                ~(x.str.contains('pin-board-main-content.html'))])
def ref_pin(x):
    return len(x[(x.str.contains('pin-board-main-content.html'))&
                ~(x.str.contains('projectId'))])

def CBI_signup(df_input, df):
    if len(df_input)>0:
        for ind, elem in enumerate(channel_list):
            for index, row in df_input[df_input['channel']==elem].iterrows():
                filter_cond = (df[col_name]==row['userId'])&(df['channel']==elem)
                df.loc[filter_cond, CBI_list[0]] = row['sign up']
    else:
        df.loc[:, CBI_list[0]] = '-'
    return df

def golden_feature(df):
    for index, row in df[['Download Report #', 'Save Report #', 'keep', 'prior']].iterrows():
        output_list =  list()
        if row['Download Report #']>0:
            output_list.append('Download Report')
        if row['Save Report #']>0:
            output_list.append('Save Report')
        if row['keep']>0:
            output_list.append('KEEP')
        if row['prior']>0:
            output_list.append('Prior Art Analytics')
        df.loc[index, 'Golden Feature'] = str(output_list)
    
    df['Golden Feature'] = [(x.split('[')[1]).split(']')[0] for x in df['Golden Feature']]
    df['Golden Feature'] = [x.replace("'", '') for x in df['Golden Feature']]      
    return df

def click_check(uilog_list, require_uilog):
    return pd.Series(uilog_list).str.contains('|'.join(require_uilog)).sum()

def CBI_tab(df_input, df, tab, mode, pattern=None):
    for ind, elem in enumerate(channel_list):
        for index, row in df_input[df_input['channel']==elem].iterrows():
            filter_cond = (df[col_name]==row['userId'])&(df['channel']==elem)
            if row['tab'] == tab:
                df.loc[filter_cond, tab + ' CBI'] = 'N'
                df.loc[filter_cond, tab + ' Time'] = row['Avg. Time (Tab)']
                df.loc[filter_cond, tab + ' # of Tab Views'] = row['Page Views']

                if row['Total Time (Tab)'] > criterion:
                    if mode==0:
                        df.loc[filter_cond, tab + ' CBI'] = 'Y'
                    elif (mode==1)&(click_check(row['uilogCode'], pattern)>=1):
                        df.loc[filter_cond, tab + ' CBI'] = 'Y'
                    elif (mode==2)&(click_check(row['uilogCode'], pattern)>=2):
                        df.loc[filter_cond, tab + ' CBI'] = 'Y'
                    else:
                        pass
    return df

def CBI_prior(df_input, df, tab):
    for ind, elem in enumerate(channel_list):
        for index, row in df_input[df_input['channel']==elem].iterrows():
            filter_cond = (df[col_name]==row['userId'])&(df['channel']==elem)
            df.loc[filter_cond, tab + ' CBI'] = 'N'
            df.loc[filter_cond, tab + ' Time'] = row['Avg. Time']
            df.loc[filter_cond,tab + ' # of Tab Views'] = row['Usage']
            if row['Result Page'] > 0:
                df.loc[filter_cond, tab + ' CBI'] = 'Y'
    return df

def CBI_keep(df_input, df):
    for index, row in df_input.iterrows():
        df.loc[df[col_name]==row['userId'], CBI_list[-3]] = row['Editable Report # of View']
        df.loc[df[col_name]==row['userId'], CBI_list[-4]] = row['KEEP Board # of View']
        df.loc[df[col_name]==row['userId'], CBI_list[-5]] = row['KEEP # of Click']
        df.loc[df[col_name]==row['userId'], CBI_list[-6]] = row['KEEP Board of Records']
    return df

def CBI_cta(df_input, df):
    for ind, elem in enumerate(channel_list):
        for index, row in df_input[df_input['channel']==elem].iterrows():
            filter_cond = (df[col_name]==row['userId'])&(df['channel']==elem)
            df.loc[filter_cond, CBI_list[-2]] = row['uilog_pricing']
            df.loc[filter_cond, CBI_list[-1]] = row['uilog_purchase']
            df.loc[filter_cond, DN_list[0]] = row['Total Time']
            df.loc[filter_cond, DN_list[1]] = row['uilog_unlock']     
            df.loc[filter_cond, DN_list[2]] = row['uilog_purchase'] 
            df.loc[filter_cond, DN_list[3]] = row['uilog_contact']
    return df

class QIMetrics(object):
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
        self.df = self.df[self.df['product']=='QI'].reset_index()
        return self.df
    
    def uilog_preprocessing(self):
        ''' uilog preprocess'''
        ## fillna visitorId by sessionId
        self.df.loc[self.df['visitorId'].isna(), 'sessionNo'] = self.df[self.df['visitorId'].isna()].groupby(['sessionId']).ngroup()
        self.df.loc[self.df['visitorId'].isna(), 'visitorId'] = self.df['sessionNo'].map(
                lambda x: 'visitor_' + str(x).split('.')[0])
        ## fillna userId by visitorId
        self.df['userId'] = self.df.groupby(['visitorId'])['userId'].fillna(method='bfill')
        self.df.loc[self.df['userId'].isna(), 'visitorNo'] = self.df[self.df['userId'].isna()].groupby(['visitorId']).ngroup()
        self.df.loc[self.df['userId'].isna(), 'userId'] = self.df['visitorNo'].map(
                lambda x: 'visitor_' + str(x).split('.')[0])
        del self.df['sessionNo']
        del self.df['visitorNo']
        
        self.df = uilog_tab(self.df)

        filter_APP = self.df['channel'] == 'Patentcloud'
        filter_DN = self.df['channel'] == 'Docket Navigator'
        filter_UP = self.df['channel'] == 'UnifiedPatents'
        filter_NA = self.df['tab'].isna()

        ##APP
        self.df.loc[filter_APP, 'appNumber'] = self.df[filter_APP]['referer'].map(
            lambda x: (x.split('appNumber=')[1]).split('&')[0] \
            if x.count('quality-insights-detail.html')>0 else None)
        ##DN
        self.df.loc[filter_DN, 'tab'] = self.df.loc[filter_DN, 'referer'].map(
                    lambda x: (x.split('com/')[1]).split('.html')[0])
        self.df.loc[filter_DN, 'tab'] = self.df.loc[filter_DN, 'tab'].map(tab_mapping)
        self.df.loc[filter_DN, 'appNumber'] = self.df.loc[filter_DN, 'referer'].map(
                    lambda x: (x.split('appNumber=')[1]).split('&')[0] if x.count('appNumber=') else None)
        ##UP
        self.df.loc[filter_UP&filter_NA, 'tab'] = 'Semantic Prior Art'
        self.df.loc[filter_UP, 'appNumber'] = self.df[filter_UP]['referer'].map(
            lambda x: (x.split('appNumber=')[1]).split('&affiliateSource')[0] \
            if x.count('appNumber=') else None)

        self.df['appNumber'] = self.df['appNumber'].map(appNumber2Format)
        self.df = self.df[~self.df['appNumber'].isna()]
        self.df['tab'].fillna('Overview', inplace=True)
        return self.df
    
    def sign_up(self):
        '''get sign up uilog'''
        col = ['userId', 'channel', 'sign up']
        output = self.df[self.df['uilogCode'].str.contains('|'.join(signup))]
        if len(output) > 0:
            output = output.groupby(['userId', 'channel']).agg(
                {'uilogCode': uilog_signup}).reset_index()
            output.columns = col
        else: 
            output = pd.DataFrame(columns=col)
        return output
    
    def prior_art(self):
        '''get PriorArt_Analysis uilog'''
        col = ['userId', 'channel', 'Usage', 'last', 
               'Total Time', 'Result Page', 'Usage', 'Avg. Time']
        output = self.df[self.df['uilogCode'].str.contains('PriorArt_Analysis')]
        if len(output) > 0:
            output = output.groupby(['userId', 'channel']).agg(
                {'sessionId': lambda x: len(set(x)), 'uilogTime': max, 
                 'stay': sum, 'uilogCode': uilog_prior}).reset_index()
            output.columns = col[:-2]
            output['Total Time'] = output['Total Time'].map(lambda x: x.seconds)
            output['Avg. Time'] = output['Total Time'] / output['Usage']
            output['Avg. Time'] =  output['Avg. Time'].map(lambda x: sec2hour(x))
        else: 
            output = pd.DataFrame(columns=col)
        return output
    
    def keep_mode(self):
        '''get keep mode uilog: only app'''
        col = ['userId', 'channel', 'KEEP # of Click', 'Editable Report # of View', 
              'KEEP Board # of View', 'KEEP Board of Records']
        output = self.df[self.df['uilogCode'].str.contains('QI_Keep')]
        if len(output) > 0:
            tmp = output[output['uilogCode']=='QI_Keep_Save'].groupby(['userId']).agg({'uilogValue': sum}).reset_index()
            output = output.groupby(['userId', 'channel']).agg(
                {'uilogCode': lambda x: len(x[x=='QI_Keep']),
                'referer': [ref_project, ref_pin]}).reset_index()
            output.columns = col[:-1]
            if len(tmp)>0:
                output = pd.merge(output, tmp, how='left', on='userId')
                output.rename(columns={'uilogValue':col[-1]}, inplace=True)
            else:
                output[col[-1]] = '-'
        else: 
            output = pd.DataFrame(columns=col)
        return output

    def cta_button(self):
        '''get CTA uilog'''
        col = ['userId', 'channel', 'Total Time', 
            'uilog_pricing', 'uilog_purchase', 'uilog_contact', 'uilog_unlock']
        output = self.df[self.df['uilogCode'].str.contains('|'.join(final))]
        if len(output)>0:
            output = output.groupby(['userId', 'channel']).agg({
                'stay': sum,
                'uilogCode': [uilog_pricing, uilog_purchase,
                            uilog_contact, uilog_unlock]}).reset_index()
            output.columns = col
            output['Total Time'] = output['Total Time'].map(lambda x: x.seconds)
            output['Total Time'] =  output['Total Time'].map(lambda x: sec2hour(x))
        else:
            output = pd.DataFrame(columns=col)
        return output

    def member_based(self, merge_df):
        '''row by users'''
        #df: uilog, merge_self: privilege
        if len(self.df)>0:
            output = self.df.groupby(['userId', 'channel', 'sessionId', 'appNumber']).agg(
                {'visitorId': 'last','country':'last', 'stay':sum,
                'tab': [lambda x: len(set(x)), list], 'uilogTime': max, 
                'uilogCode': [uilog_download, uilog_save, uilog_keep, uilog_prior]}).reset_index()
            output.columns = ['userId', 'channel', 'sessionId', 'appNumber',
                          'visitorId', 'Country', 'Total Time',
                           '# of Total Tab Views', 'Tab Visited', 'uilogTime',
                           'Download Report #', 'Save Report #', 'keep', 'prior']
            output = output.groupby(['userId', 'channel']).agg(
                {'visitorId': 'last', 'Country':'last', 'Total Time':sum,
                '# of Total Tab Views': sum, 'Tab Visited': sum,
                'uilogTime': max, 
                'appNumber': lambda x: list(set(x)),
                'Download Report #': sum, 'Save Report #': sum,
                'keep': sum, 'prior': sum}).reset_index()
            
            output.loc[output['Country']=='', 'Country'] = '-'
            output['Total Time'] = output['Total Time'].map(lambda x: x.seconds)
            output['Total Time'] = output['Total Time'].map(lambda x: sec2hour(x))
            output['Update Date'] = output['uilogTime'].map(lambda x: str(x)[:11])
            output['# of Checked Reports'] = output['appNumber'].map(len)
            output['appNumber'] = [','.join(map(str, x)) for x in output['appNumber']]
            output['Tab Visited'] = output['Tab Visited'].map(lambda x: [xx for xx in x if xx != None])
            output['Tab Visited'] = output['Tab Visited'].map(lambda x: list(set(x)))
            output['Tab Visited'] = [','.join(map(str, x)) for x in output['Tab Visited']]
            output = golden_feature(output)
            
            if len(merge_df)>0:
                output = pd.merge(output, merge_df, how='left', left_on='userId', right_on='member_id')
                del output['member_id']
            else:
                col_list = ['product_code', 'auth_start_date',
                    'auth_end_date', 'id', 'Account', 'Registration Date',
                    'Registration Source', 'Auth Type', 'Company', 'Job Title',
                    'Industry Classification', 'Corporate', 'inner', 'channel', 'Trial Status']
                for col in col_list:
                    if col not in output.columns:
                        output[col] = '-'
        return output

    def tab_based(self, merge_df):
        '''row by tabs'''
        #self.df: uilog, merge.df: member_based
        if len(self.df)>0:
            output = self.df.groupby(['userId', 'channel', 'tab', 'appNumber']).agg(
                {'sessionId': lambda x: len(set(x)), 
                'uilogCode': lambda x: list(itertools.chain(x)),
                'uilogTime': max, 'stay': sum}).reset_index()
            output = output.groupby(['userId', 'channel', 'tab']).agg(
                {'sessionId': sum, 'uilogCode': sum,
                'uilogTime': max, 'stay': sum}).reset_index()
            output.rename(columns={
                 'sessionId':'Page Views', 
                'uilogTime':'last', 'stay':'Total Time (Tab)'}, inplace=True)

            output['Total Time (Tab)'] = output['Total Time (Tab)'].map(lambda x: x.seconds)
            output['Avg. Time (Tab)'] = output['Total Time (Tab)'] / output['Page Views']
            output['Avg. Time (Tab)'] = output['Total Time (Tab)'].map(lambda x: sec2hour(x))
            output = pd.merge(merge_df, output, on=['userId', 'channel'])
        return output
    
    def CBI(self, raw, output):
        #raw: tab-based, output: member-based
        #init.
        for col in column_list:
            if col not in output.columns:
                output[col] = '-'
        output.loc[:, 'Tier'] = ''
        output.loc[:, 'CSD'] = ''
        output.loc[:, 'Group'] = ''

        # Identity
        output.loc[~output['userId'].str.contains('visitor'), 'Identity'] = 'MQL'

        id_list = [x for x in set(raw['userId'])]
        for index, element in enumerate(id_list):
            #Prior Art Anaytics
            output = CBI_prior(self.prior_art(), output, 'Prior Art Analytics')
            #sign up
            output = CBI_signup(self.sign_up(), output)
            #keep mode
            output = CBI_keep(self.keep_mode(), output)
            #CTA
            output = CBI_cta(self.cta_button(), output)

            tmp = raw[raw[col_name] == element]
            #Summary
            output = CBI_tab(tmp, output, 'Overview', mode=2, pattern=overview)
            #History
            output = CBI_tab(tmp, output, 'History', mode=1, pattern=history)
            #Claim Analysis
            output = CBI_tab(tmp, output, 'Claim Analysis', mode=1, pattern=analysis)
            #Claim Insights
            output = CBI_tab(tmp, output, 'Claim Insights', mode=1, pattern=insights)
            #Family Prior Art
            output = CBI_tab(tmp, output, 'Family Prior Art', mode=2, pattern=family)
            #Prior Art Finder
            output = CBI_tab(tmp, output, 'Prior Art Finder', mode=2, pattern=finder)   
            #Semantic Prior Art
            output = CBI_tab(tmp, output, 'Semantic Prior Art', mode=0)
            #File Wrapper
            output = CBI_tab(tmp, output, 'File Wrapper Search', mode=1, pattern=wrapper) 
        return output

    def PQL(self, output):
        # check overall CBI   
        output['Y'] = output.loc[:,CBI_list].apply(lambda x: Counter(x).get('Y'), axis=1)
        output['Y'].fillna(0, inplace=True)

        #identify PQL
        output.loc[(output['Identity']=='MQL')&(output['# of Checked Reports']>=2)&
             (output['# of Total Tab Views']>=5)&(output['Y']>=3), 'PQL'] = 'Y'
        output['PQL'].fillna('N', inplace=True)
        return output
    
    def output_format(self, output):
        #rename
        output.rename(columns={'userId': 'Member ID',
                          'visitorId': 'Visitor ID',
                          'channel': 'Traffic Source',
                          'appNumber': 'Checked Reports'}, inplace=True)
        
        # final tune
        output = output[output['inner']!=1]
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
                 'Update Date', '# of Checked Reports', 'Checked Reports', 
                 '# of Total Tab Views', 'Total Time', 'Tab Visited', 'Golden Feature',
                *column_list]]
        output.sort_values(by=['PQL', 'Identity'], ascending=[False, False], inplace=True)
        output = output.reset_index(drop=True)
        return output

if __name__ == '__main__':
    # define parameter
    now = datetime.today()

    start = str(now - timedelta(days=1))[:10] + ' 00:00:00'
    end = str(now - timedelta(days=0))[:10] + ' 00:00:00'
    startDate = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    endDate = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
    now = datetime.strptime(str(now)[:10], '%Y-%m-%d')

    Query = '''insert into CBI_QI (PQL,visitorId, userId,
                    traffic_source, trial_status, created_date,
                    checked_reports_num, checked_reports, tab_num,
                    total_time, tab_visited, golden_feature, sign_up,
                    O_CBI, O_time, O_view, H_CBI, H_time, H_view,
                    CA_CBI, CA_time, CA_view, CI_CBI, CI_time, CI_view,
                    F_CBI, F_time, F_view, PA_CBI, PA_time, PA_view,
                    S_CBI, S_time, S_view, W_CBI, W_time, W_view,
                    A_CBI, A_time, A_view,
                    download_num, save_num, editable_num,
                    keep_click, keep_view, keep_record,
                    subscribe_click, shooping_view,
                    unlock_time, unlock_view, unlock_purchase, unlock_contact)
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''

    try: 
        #uilog
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
            QI = QIMetrics(uilog)
            uilog = QI.uilog_format()
            if len(uilog)>0:
                uilog = QI.uilog_preprocessing()

                id_list = [str(x) for x in set(uilog[uilog['product']=='QI']['userId']) if 'visitor' not in x]
                privilege = a.CBI_Privilege(['Lic_QI_FREE_TRIAL_DOCKET_NAVIGATOR', 'Lic_QI_Free_Trial'], id_list)
                privilege = a.CBI_Member(id_list, privilege)
                
                member_df = QI.member_based(privilege)
                tab_df = QI.tab_based(member_df)
                output = QI.CBI(tab_df, member_df)
                output = QI.PQL(output)
                output = QI.output_format(output)

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
        print(str(now)+' finish!')

    except BaseException as e:
        print('fail!)
