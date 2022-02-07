from pandas.core.groupby.grouper import Grouper
from pandas.core.indexes.base import Index
import streamlit as st
import json
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from PIL import Image
from annotated_text import annotated_text
import numpy as np
import os 
key_dict_bq = json.loads(st.secrets["textkey_bq"])
creds_bq = service_account.Credentials.from_service_account_info(key_dict_bq)
#os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = creds_bq
from google.cloud import firestore
key_dict_fq = json.loads(st.secrets["textkey_fb"])
creds_fb = service_account.Credentials.from_service_account_info(key_dict_fq)
db = firestore.Client(credentials=creds_fb )

st.set_page_config(page_title='atyp\'s Search Ads Budget Pacing Dock', page_icon='/Atyp_RGB-POS.png', layout='wide')

##MainMenu {visibility:hidden; }
hide_menu_style = """
    <style>
    
    footer {visibility:hidden;}
    """
st.markdown(hide_menu_style, unsafe_allow_html= True)

credentials = creds_bq
client = bigquery.Client(credentials=credentials)

@st.cache(ttl=600)
def run_query(query):
    query_job = client.query(query)
    rows_raw = query_job.result()
    # Convert to list of dicts. Required for st.cache to hash the return value.
    rows = [dict(row) for row in rows_raw]
    
    return rows

rows = pd.read_gbq("SELECT * FROM `e-quanta-287411.dbt_pedro_atyp.streamlit_adapter`", credentials=credentials,)
image = Image.open('/Users/pedro/streamlitdash/Atyp_RGB-POS.png')

st.sidebar.image(image, use_column_width=True)
st.sidebar.title('atyp\'s budget pacing tool')
option = st.sidebar.selectbox(
    'Which account would you like to see?',
    (rows.account_name.unique()))


st.sidebar.write('Account:', option)
st.sidebar.subheader("Select the amount and the month you would like to update")
df = (rows.account_name == option)
Mdf = pd.DataFrame(rows[df])
adf = Mdf.loc[:, ['date_day', 'account_name', 'account_id', 'campaign_name', 'campaign_id', 'spend', 'budget']]


account_name = option
@st.cache(allow_output_mutation=True)
def run_firestone():
    docs = db.collection("searchBudgets").where('account_name', '==', option).stream()
    m = []
    for docs_dict in docs:
        doc = docs_dict.to_dict()
        
        for i in doc['budget']:
            onemonth = i
            
            m.append(onemonth)
    
    return(m)

months = run_firestone()


firebasedata = st.sidebar.form(key="Options")
newbudget = firebasedata.number_input("Amount")
month = firebasedata.selectbox("Month", months)

submit = firebasedata.form_submit_button("Submit new budget")  


today = pd.Timestamp('today') 
current_month = today.month
current_year = today.year

cost_time_series = adf[['date_day', 'spend']].groupby('date_day').sum().round(0)

adf['year'] = adf['date_day'].dt.year
adf['month'] = adf['date_day'].dt.month
adf['days_in_month'] = adf['date_day'].dt.days_in_month
adf['current_days_elapsed'] = today.day
adf['account_budget'] = adf['budget'].sum()/len(adf.budget)
adf['monthy_budget'] = adf['days_in_month']*adf['budget']


pacing_conditions = [

    np.logical_and(adf['month'] == current_month, adf['year'] == current_year )
]

pacing_choices = [ ((adf['month']/adf['current_days_elapsed']) * (adf['spend']/adf['monthy_budget']))]

adf['budget_pacing'] = np.select(pacing_conditions, pacing_choices, default=adf['month']/adf['month']) * (adf['spend']/adf['monthy_budget'])




account_df = adf.loc[:, ['year', 'month', 'account_name','campaign_name', 'spend', 'budget']]



df_budget =adf[['date_day', 'account_name','campaign_name','budget']].groupby(['account_name',pd.Grouper(key='date_day', freq='M')]).apply( lambda x: (x['budget'].sum()/len(x))*adf['campaign_name'].nunique()).reset_index(name='avg_daily_budget')
df_spend = adf[['date_day', 'account_name','campaign_name','spend']].groupby(['account_name', pd.Grouper(key='date_day', freq='M')]).sum().reset_index()
df_merged = df_spend.merge(df_budget).sort_values(by='date_day', ascending=False)


df_merged['year'] = df_merged['date_day'].dt.year
df_merged['month'] = df_merged['date_day'].dt.month
df_merged['days_in_month'] = df_merged['date_day'].dt.days_in_month
df_merged['current_days_elapsed'] = today.day
df_merged['monthly_budget'] = df_merged['avg_daily_budget']*df_merged['days_in_month']
df_merged['prc_buget_used'] = df_merged['spend']/df_merged['monthly_budget']


pacing_conditions = [

    np.logical_and(df_merged['month'] == current_month, df_merged['year'] == current_year )
]

pacing_choices = [ ((df_merged['days_in_month']/df_merged['current_days_elapsed']) * (df_merged['prc_buget_used']))]

df_merged['budget_pacing'] = np.select(pacing_conditions, pacing_choices, default=df_merged['days_in_month']/df_merged['days_in_month']) * (df_merged['spend']/df_merged['monthly_budget'])
df_merged['month_name'] = df_merged['date_day'].dt.month_name()

top_table = st.container()





with top_table:
#     st.write('##')
#     annotated_text(
#     "This ",
#     ("is", "verb", "#8ef"),
#     " some ",
#     ("annotated", "adj", "#faa"),
#     ("text", "noun", "#afa"),
#     " for those of ",
#     ("you", "pronoun", "#fea"),
#     " who ",
#     ("like", "verb", "#8ef"),
#     " this sort of ",
#     ("thing", "noun", "#afa"),
    
# )

    st.title('atyp\'s budget pacing tool for Google Search Ads')
    st.markdown('Internal dashboard for **_budget pacing_**')
    st.markdown('Data sources are **Google ads** search and **atyp\'s** mini data-lake')
    st.write('##')
    st.subheader(f'Buget data for Account: {option}')
    st.line_chart(cost_time_series)
    st.write('##')
    st.subheader('Budget Pacing Account Level')
    st.markdown(f'Account: **{option}**')
    # st.write(df_merged)
    # st.subheader('Budget Pacing Campaign Level')
    # st.write(adf)

    





with st.container():
    st.title('Monthly set up budget') 
    col1, col2 = st.columns(2)
# Note: Use of CollectionRef stream() is prefered to get()


documentos = db.collection("searchBudgets").where('account_name', '==', option).stream()



monthBudget_dic_per_month = []
for ii in documentos:
    account = ii.to_dict()
    account_name = account["account_name"]
    budget_January = account['budget']['January']
    budget_February = account['budget']['February']
    budget_March = account['budget']['March']
    budget_April = account['budget']['April']
    budget_May = account['budget']['May']
    budget_June = account['budget']['June']
    budget_July = account['budget']['July']
    budget_August = account['budget']['August']
    budget_September = account['budget']['September']
    budget_October = account['budget']['October']
    budget_November = account['budget']['November']
    budget_December = account['budget']['December']
    for j in account['budget']:
        mp_month = j
        mp_budget = account['budget'][j]
        

        dataFinal = {
        'month_name' : mp_month,
        'mpBuget' : mp_budget,
        'account_name' :  account_name      
            } 

        monthBudget_dic_per_month.append(dataFinal)

    
    
    col1.subheader(f"Account: {account_name}, (from Media Plan)")
    col1.write(f"January: {budget_January}")
    col1.write(f"February: {budget_February}")
    col1.write(f"March: {budget_March}")
    col1.write(f"April: {budget_April}")
    col1.write(f"May: {budget_May}")
    col1.write(f"June: {budget_June}")
    col1.write(f"July: {budget_July}")
    col1.write(f"August: {budget_August}")
    col1.write(f"September: {budget_September}")
    col1.write(f"October: {budget_October}")
    col1.write(f"November: {budget_November}")
    col1.write(f"December: {budget_December}")

    budget_by_month= []
    



    monthName = df_merged.loc[df_merged['year'] == 2022, 'month_name'].to_list()
    amount_budget = df_merged['monthly_budget'].to_list()
    amount_spent = df_merged['spend'].to_list()
    #         # df_spend = i[k]['spend']
            # ano = i[k]['year']


    # for i in df_merged:
    #     monthName = i[i]
    #     # amount_budget = i['monthly_budget']
    #     # amount_spent = i['spend'] 

    dataFinalbud = {
            'month_name' : monthName,
            'amount' : amount_spent,
            'amount_budget' :  amount_budget
    
            } 


    budget_by_month.append(dataFinalbud)
    print(budget_by_month)

    col2.subheader(f"Account: {account_name}, (from Platform)")
    zz = 0
    for i in budget_by_month[0]['month_name']:
    
        month_name = budget_by_month[0]['month_name'][zz]
        amount = budget_by_month[0]['amount'][zz]
        amount_budget = budget_by_month[0]['amount_budget'][zz]

        col2.write(str(month_name)+" Budget " + ": "  + str(amount_budget) + " NOK " + "--->   SPEND " + str(amount)+" NOK")  
        
        zz += 1
        
        # dataList = [{'a': 1}, {'b': 3}, {'c': 5}]
        # for index in range(len(dataList)):
        #     for key in dataList[index]:
        #         print(dataList[index][key])
        
        # for zz in 
        # col2.write(f"{i['month_name']} :  {i['amount_budget']}")    


dataframe_like_per_month = pd.DataFrame(monthBudget_dic_per_month)
dataframe_like_per_month['year'] =today.year
last_merge = df_merged.merge(dataframe_like_per_month, how="left", on=["month_name", "year"]).sort_values(by='date_day', ascending=False)
last_merge['prc_buget_used_MP'] = last_merge['spend']/last_merge['mpBuget']



pacing_conditions_2 = [

    np.logical_and(last_merge['month'] == current_month, last_merge['year'] == current_year )
]

pacing_choices_2 = [ ((last_merge['month']/last_merge['current_days_elapsed']) * (last_merge['prc_buget_used_MP']))]

last_merge['budget_pacing_MP'] = np.select(pacing_conditions_2, pacing_choices_2, default=last_merge['month']/last_merge['month']) * (last_merge['spend']/last_merge['mpBuget'])




all_data = last_merge[['account_name_x', 'date_day',  'spend',  'avg_daily_budget',  'year', 'month_name'  , 'monthly_budget',  'prc_buget_used' , 'budget_pacing' , 'mpBuget', 'budget_pacing_MP', 'prc_buget_used_MP' ]].rename(columns={"account_name_x": "account_name", "mpBuget": "media_plan_budget", "budget_pacing" : "budget_pacing_Plat", "monthly_budget" : "monthly_budget_platform"})

print(all_data)

#all_data[['spend','avg_daily_budget','prc_buget_used', 'monthly_budget_platform', 'budget_pacing_Plat', 'media_plan_budget', 'prc_buget_used_MP', 'budget_pacing_MP' ]] = all_data[['spend','avg_daily_budget','prc_buget_used', 'monthly_budget_platform', 'budget_pacing_Plat', 'media_plan_budget', 'prc_buget_used_MP', 'budget_pacing_MP' ]].style.format('{0:,.2f}')


top_table.dataframe(df_budget)

top_table.dataframe(adf)

top_table.dataframe(all_data)#.style.format(formatter:{('spend','avg_daily_budget','prc_buget_used', 'monthly_budget_platform', 'budget_pacing_Plat', 'media_plan_budget', 'prc_buget_used_MP', 'budget_pacing_MP' : )   ]]('{0:,.2f}'))

if newbudget and month and submit:
    #st.session_state.counter = True
    print("inside cocksucker")
    doc_ref = db.collection("searchBudgets").document(ii.id)
    doc_ref.update({
        "budget." + month : newbudget
    })

    st.balloons()
        