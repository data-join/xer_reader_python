import sys
import pandas as pd
import altair as alt
import streamlit as st
import numpy as np
from sqlalchemy import create_engine
engine = create_engine('sqlite://', echo=False)
def clean(grouped,table):
    DF = grouped.get_group(table)
    DF.dropna(axis=1, how='all',inplace=True)
    new_header = DF.iloc[0] #grab the first row for the header
    DF = DF[1:] #take the data less the header row
    DF.columns = new_header #set the header row as the df header
    del DF['%F']
    return DF
@st.cache
def load_data(uploaded_file):
    df = pd.read_csv(uploaded_file,sep='\t',names=range(100), encoding= 'unicode_escape',dtype=str)
    df.loc[df[0] == '%T', 'table'] = df[1]
    df['table'].fillna(method='ffill', inplace=True)
    data=df.loc[df[0].isin(['%R','%F'])]
    return data
st.sidebar.title("About")
st.sidebar.info(
        "This app is a simple example of "
        "using Streamlit to create web app.\n"
        "\nIt is maintained by [Mim]("
        "https://datamonkeysite.com/about/).\n\n"
        "https://github.com/djouallah/xer_reader_python"
    )
st.title('Read XER')
uploaded_file = st.file_uploader("Choose an XER file", type="xer")
if uploaded_file is not None:
    dff=load_data(uploaded_file)
    tablelist= dff['table'].unique()
    tables =pd.DataFrame(tablelist)
    tables.columns = ['tables']
    grouped = dff.groupby(dff.table)
    df_list= {}
    #####################################
    for x in tablelist:
        df = clean(grouped,x)
        df.to_sql(x, con=engine)
    PROJECT =pd.read_sql("SELECT proj_id,proj_short_name FROM PROJECT",engine) 
    values = PROJECT['proj_short_name'].tolist()
    options = PROJECT['proj_id'].tolist()
    dic = dict(zip(options, values))
    proj_id_var= st.sidebar.selectbox('Select Project', options, format_func=lambda x: dic[x])
    ###### Shwing sme stats about the file
    result1 =pd.read_sql("SELECT 'Data Date' as Project, [last_recalc_date] as Date FROM PROJECT where proj_id="+proj_id_var,engine)
    result2 =pd.read_sql("select 'Project Start' as Project ,min([project_start]) as Date \
    from(SELECT min([act_start_date]) as Project_Start FROM TASK where proj_id="+proj_id_var+ \
    " UNION ALL SELECT min([early_start_date]) as Project_Start FROM TASK where proj_id="+proj_id_var+")" ,engine)
    result3 =pd.read_sql("select 'Project Finish' as Project ,max([project_Finish]) as Date \
    from(SELECT max([act_end_date]) as Project_Finish FROM TASK where proj_id="+proj_id_var+ \
    " UNION ALL SELECT max([late_end_date]) as Project_Finish FROM TASK where proj_id="+proj_id_var+")" ,engine)
    pv=pd.concat([result1, result2,result3], axis=0)
    pv.set_index('Project', inplace=True)
    st.sidebar.table (pv)
    ###### Shwing sme stats about the file
    result =pd.read_sql("SELECT null as id,count(*) as Total_Task, \
    sum(case when [task_type] = 'TT_Task' then 1 else 0 end ) as Task_Dependant, \
    sum(case when [task_type] = 'TT_WBS' then 1 else 0 end ) as WBS_Summary, \
    sum(case when [task_type] = 'TT_LOE' then 1 else 0 end ) as LOE, \
    sum(case when [task_type] = 'TT_Mile' then 1 else 0 end ) as Start_Milestone, \
    sum(case when [task_type] = 'TT_FinMile' then 1 else 0 end ) as Finish_Milestone, \
    sum(case when [task_type] = 'TT_Rsrc' then 1 else 0 end ) as Resource_Dependant, \
    sum(case when [status_code] = 'TK_Complete' then 1 else 0 end ) as Completed, \
    sum(case when [status_code] = 'TK_Active' then 1 else 0 end ) as On_Going, \
    sum(case when [status_code] = 'TK_NotStart' then 1 else 0 end ) as Not_Started, \
    sum(case when [total_float_hr_cnt] = '0' then 1 else 0 end ) as Critical, \
    sum(case when cast([total_float_hr_cnt] as real) < 0 then 1 else 0 end ) as Negative_Float, \
    sum([target_work_qty])  as Budget_Labor, \
    sum([act_work_qty])  as Actual_Labor, \
    sum([remain_work_qty])  as Remaining_Labor, \
    sum([remain_work_qty]) + sum([act_work_qty]) as at_Completion_Labor, \
    sum([target_equip_qty])  as Budget_Non_Labor, \
    sum([act_equip_qty])  as Actual_Non_Labor, \
    sum([remain_equip_qty])  as Remaining_Non_Labor, \
    sum([remain_equip_qty]) + sum([act_work_qty]) as at_Completion_Non_Labor \
    FROM TASK where proj_id="+proj_id_var,engine) 
    pv = result.melt(id_vars=["id"], 
        var_name="Task", 
        value_name="Value")
    pv=pv[['Task','Value']]
    pv.set_index('Task', inplace=True)
    st.subheader("show some stats about the file")  
    st.dataframe (pv)
    ###### Shwing sme stats about the file
    result =pd.read_sql("SELECT [task_code] as Activity_ID, [task_name] as Activity_Name FROM TASK where proj_id="+proj_id_var+" and cast([total_float_hr_cnt] as real) < 0 ",engine)
    if not result.empty:
        st.subheader("Task with negative float")  
        st.dataframe (result)