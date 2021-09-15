'''main and monitor by Slack Webhook'''
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, date
from openpyxl import load_workbook
from xlsxwriter.workbook import Workbook
from inspect import currentframe, getframeinfo
from requests import post
import warnings
warnings.filterwarnings('ignore')
from QI import *
from SEP import *
from GetData import *
from Preprocessing import *
from Update2Sheet import *

def divide_data(query, output):
    '''split data for uploading sheet and DB'''
    drop_index = ['Tier', 'Identity', 'CSD', 'Group',
                'Registration Source', 'Job Title',
                'Industry Classification', 'Registration Date',
                'Country', 'Company', 'Corporate']
    sheet_index = ['Member ID', 'Tier', 'Identity', 'CSD', 'Group']
    output1 = output.loc[output['Member ID']!='-', sheet_index]
    output1.drop_duplicates('Member ID', inplace=True)
    output2 = output.drop(columns=drop_index)

    a.CBI_upload(output1)
    a.CBI_DB(query, output2)
    
# define parameter
slackURL = ''
log = {}
now = datetime.today()

start = str(now - timedelta(days=1))[:10] + ' 00:00:00'
end = str(now - timedelta(days=0))[:10] + ' 00:00:00'
startDate = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
endDate = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
now = datetime.strptime(str(now)[:10], '%Y-%m-%d')

Query_QI = '''insert into CBI_QI (PQL,visitorId, userId,
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

Query_SEP = '''insert into CBI_SEP (PQL, visitorId, userId,
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

try: 
    # uilog
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
        SEP = SEPMetrics(uilog)
        uilog_QI = QI.uilog_format()
        uilog_SEP = SEP.uilog_format()
        if len(uilog_QI)>0: 
            uilog_QI = QI.uilog_preprocessing()
            
            id_list = [str(x) for x in set(uilog_QI['userId']) if 'visitor' not in x]
            privilege_QI = a.CBI_Privilege(['Lic_QI_FREE_TRIAL_DOCKET_NAVIGATOR', 'Lic_QI_Free_Trial'], id_list)
            privilege_QI = a.CBI_Member(id_list, privilege_QI)

            member_QI = QI.member_based(privilege_QI)
            tab_QI = QI.tab_based(member_QI)
            output_QI = QI.CBI(tab_QI, member_QI)
            output_QI = QI.PQL(output_QI)
            output_QI = QI.output_format(output_QI)

            divide_data(Query_QI, output_QI)
            print('QI finish!')

        if len(uilog_SEP)>0:
            uilog_SEP = SEP.uilog_preprocessing()
            
            id_list = [str(x) for x in set(uilog_SEP['userId']) if 'visitor' not in x]
            privilege_SEP = a.CBI_Privilege(['Lic_SEP_FREE_TRIAL'], id_list)
            privilege_SEP = a.CBI_Member(id_list, privilege_SEP)        

            member_SEP = SEP.member_based(privilege_SEP)
            tab_SEP = SEP.tab_based(member_SEP)
            output_SEP = SEP.CBI(tab_SEP, member_SEP)
            output_SEP = SEP.PQL(output_SEP)
            output_SEP = SEP.output_format(output_SEP)
 
            divide_data(Query_SEP, output_SEP)
            print('SEP finish!')

    # monitor for Slack Webhook
    log['text'] = 'CBI Data successfully insert on @{0}'.format(date.today().strftime('%b %d %Y'))
    post(slackURL, json=log)

except BaseException as e:
    msg = "CBI Data fail:\n"+str(e)
    log['text'] = msg 
    post(slackURL, json=log)
