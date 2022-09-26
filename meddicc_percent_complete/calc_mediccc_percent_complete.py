#%%
# import libraries 
from datetime import datetime

import pandas as pd 

from salesforce.salesforce_setup import MySalesForce



# query open opportunities 
def get_open_opportunities():
    # get open opportunities DataFrame 
    df = MySalesForce.sfdc_query(
        """
        SELECT
            Id,
            Metrics__c,
            Economic_Buyer__c,
            Decision_Criteria__c,
            Decision_Process__c,
            Paper_Process__c,
            Identify_Pain__c,
            Champion__c,
            Compelling_Event__c,
            Competitors__c
        FROM 
            Opportunity
        WHERE 
            IsClosed = FALSE
        """
    )
    # return DataFrame 
    return df 

def calc_mediccc_percent_complete(df):
    # calc mediccc percent complete 
    df["MEDICCC_Complete__c"] = (df.notna().sum(axis=1) / 9).round(2) * 100
    # return DataFrame with MEDICCC % Complete Column 
    return df 

def upload_mediccc_percent_complete(df):
    # create uplaod dictionary 
    upload_dict = df[['Id','MEDICCC_Complete__c']].to_dict(orient='records')

    # upload to SFDC 
    res = MySalesForce.sf.bulk.Opportunity.update(upload_dict)

    # get results DataFrame 
    res_df = pd.DataFrame.from_records(res)

    # print results 
    print('Results:')
    print(res_df.success.value_counts())
    print('\n')

    # print errors 
    print('Errors:')
    print(res_df[res_df.errors.apply(lambda x: len(x) > 0)].errors.value_counts())

    # store results 
    res_df.to_csv('results_' + str(datetime.today().date()).replace('-','_') + '.csv')