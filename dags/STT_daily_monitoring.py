from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from plugins.connections import Connections
from datetime import datetime, timedelta
import pandas as pd
   

def create_info_table(disp_transcribations, checked_kd_disp_transcribations, start, tablename):
    start = datetime.now() - timedelta(days=start)
    end = datetime.now()
    start = datetime.strptime(start.strftime("%Y-%m-%d"), "%Y-%m-%d")
    end = datetime.strptime(end.strftime("%Y-%m-%d"), "%Y-%m-%d")
    date_generated = [start + timedelta(days=x) for x in range(0, (end-start).days)]
    df = pd.DataFrame(date_generated, columns=['Дата'])
    df['Дата'] = df['Дата'].astype(str)
    disp_transcribations_df = pd.read_sql(disp_transcribations, con=Connections.SQLServerconn())
    disp_transcribations_df['Дата'] = disp_transcribations_df['Дата'].astype(str)
    checked_kd_disp_transcribations_df = pd.read_sql(checked_kd_disp_transcribations, con=Connections.SQLServerconn())
    checked_kd_disp_transcribations_df['Дата'] = checked_kd_disp_transcribations_df['Дата'].astype(str)
    res_df = df.merge(disp_transcribations_df, how='left', left_on='Дата', right_on='Дата')
    res_df = res_df.merge(checked_kd_disp_transcribations_df, how='left', left_on='Дата', right_on='Дата')
    res_df = res_df.fillna(0)
    res_df.iloc[:,1:] = res_df.iloc[:,1:].astype(int)
    res_df['Дата'] = pd.to_datetime(res_df['Дата'], format='%Y-%m-%d')
    res_df['SYS_MOMENT'] = datetime.now()
    res_df.to_sql(tablename, con=Connections.postgresconn(), if_exists='replace', index=False)

default_args = {'owner': 'Samigullin_Ildus',
            'depends_on_past': False,
            'retries': 0,
            'retry_delay': timedelta(seconds=120)
            }

with DAG(dag_id="STT_daily_monitoring",
        tags = ['STT', 'monitoring'],
        description = "Мониторинг кол-ва оцененных и транскрибированных записей" ,
        schedule_interval="0 6 * * *", 
        start_date=days_ago(1),
        catchup=False,
        default_args=default_args) as dag:

    create_info_table = PythonOperator(
            task_id='create_info_table',
            python_callable=create_info_table,
            op_kwargs={'disp_transcribations' : """ 
                                                SELECT CAST(created as DATE) AS [Дата]
                                                ,COUNT(DISTINCT audio_oktell_id) AS [Транскрибировано звонков диспетчеров]
                                                FROM [dbo].[STT_Transcribations_test]
                                                WHERE DATEDIFF(DAY, created, GETDATE()) BETWEEN 1 AND 28
                                                GROUP BY CAST(created as DATE)
                                                ORDER BY [Дата] 
                                                """,                                
                        'checked_kd_disp_transcribations' : """
                                                SELECT CAST(SYS_MOMENT AS date) AS [Дата]
                                                ,COUNT(DISTINCT Call_Id) AS [Оценено транскрибаций диспетчеров КД]
                                                FROM [Analytic].[dbo].[KD_Disp_tr_check]
                                                WHERE DATEDIFF(DAY, SYS_MOMENT, GETDATE()) BETWEEN 1 AND 28
                                                GROUP BY CAST(SYS_MOMENT AS date)
                                                ORDER BY [Дата] 
                                                """,
                        'start' : 27,
                        'tablename' : 'STT_daily_monitoring'})

    create_info_table
