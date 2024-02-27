import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'https://storage.yandexcloud.net/kc-startda/top-1m.csv'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

#получение данных
def get_data():
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

#топ-10 доменных зон по численности доменов
def top10_domain_zone():
    top10_domain_zone = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top10_domain_zone['domain_zone'] = top10_domain_zone.domain.str.split('.').str[-1]
    top10_domain_zone = top10_domain_zone.groupby('domain_zone', as_index = False)\
                                    .agg({'domain' : 'count'})\
                                    .sort_values('domain', ascending = False)\
                                    .head(10)
    with open('top10_domain_zone.csv', 'w') as f:
        f.write(top10_domain_zone.to_csv(index=False, header=False))

#домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
def longest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    longest = top_data_df.domain.str.len().max()
    longest_domain = top_data_df.query('domain.str.len() == @longest').sort_values(by=['domain']).head(1).domain
    with open('longest_domain.csv', 'w') as f:
        f.write(longest_domain.to_csv(index=False, header=False))

#на каком месте находится домен airflow.com?
def airflow_domain_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank =  top_data_df.query('domain == "airflow.com"')
    airflow_domain_rank = airflow_rank['rank']
    with open('airflow_domain_rank.csv', 'w') as f:
        f.write(airflow_domain_rank.to_csv(index=False, header=False))

#вывод данных 
def print_data(ds):
    with open('top10_domain_zone.csv', 'r') as f:
        domain_zone_data = f.read()
    with open('longest_domain.csv', 'r') as f:
        longest_domain_data = f.read()
    with open('airflow_domain_rank.csv', 'r') as f:
        airflow_domain_rank_data = f.read()
    date = ds

    print(f'Топ-10 доменных зон по численности доменов на {date}')
    print(domain_zone_data)

    print(f'Домен с самым длинным именем на {date}')
    print(longest_domain_data)
    
    print(f'Позиция домена airflow.com на {date}')
    print(airflow_domain_rank_data)


default_args = {
    'owner': 'ek-drozd',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 11, 8),
}
schedule_interval = '0 12 * * *'

dag = DAG('ek-drozd_DAG_lesson2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top10_domain_zone',
                    python_callable=top10_domain_zone,
                    dag=dag)

t3 = PythonOperator(task_id='longest_domain',
                        python_callable=longest_domain,
                        dag=dag)

t4 = PythonOperator(task_id='airflow_domain_rank',
                        python_callable=airflow_domain_rank,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t2, t3, t4] >> t5
