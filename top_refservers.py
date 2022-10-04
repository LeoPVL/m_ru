import requests
import pandas as pd
import datetime
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from pulse_other import pulse_clickhouse

key = 'xxxxxxx'
headers = {'X-Authkey': key}
count_dict = {'media_ediniy + desktop + total': '282',
              'media_ediniy + desktop + main page': '3230022',
              'media_ediniy + desktop + pulse': '3230024',
              'media_ediniy + desktop + google': '2957929',
              'media_ediniy + desktop + yandex': '2957928'
              }


def get_last_date_from_table(counter):
    sql = """ 
    select case when date='1970-01-01' then toDate('2020-12-31') else date end as date
    from(select max(date) as date from pulse.top_refservers_replicated_distributed
    where counter = '{}')    
    """.format(counter)

    names = ['date']
    df_stat = pulse_clickhouse.get_df_from_ch(sql,names)

    return datetime.datetime.strptime(df_stat['date'].values[0], '%Y-%m-%d').date()

def get_info_from_api():
    df_sources = pd.DataFrame()

    for key, value in count_dict.items():
        print(value)
        # date_start = date(2021,1,1)#get_last_date_from_table(value) + relativedelta(days=1)
        date_start = get_last_date_from_table(value) + relativedelta(days=1)
        print(date_start)
        #date_end = date(2022,1,1)#date.today() # до какого дня date(2022,4,2) (не включительно)
        date_end = date.today()  # до какого дня date(2022,4,2) (не включительно)
        days_delta = (date_end - date_start).days
        if days_delta <= 0:
            print('Data already in table')
            continue

        for delta in range(days_delta):
            current_date = date_start + timedelta(days = delta)
            sd = current_date.strftime('%Y-%m-%d') # дата
            print(sd)

            data = requests.get(f'http://top.mail.ru/json/refservers',
                            params={'id': value,
                                    'date': sd,
                                    'period': 0,# 0 - дни, 1 - недели, 2 - месяцы
                                    'pp': 500
                                    },
                            headers=headers
                            #,proxies={ "https" : "http://srch-proxy-common.g:3128","http" : "http://srch-proxy-common.g:3128"}
                            ).json()

        #     time.sleep(0.2) # если не делать паузы, будет ошибку доступа
            try:
                df = pd.DataFrame.from_records(data['elements'])
                if data["pager_count"] > 500:
                    raise ValueError('pager_count>500 data will be lost need to rewrite get_info_from_api')

                df['project'] = key.split(' + ')[0]
                df['device'] = key.split(' + ')[1]
                df['source_counter'] = key.split(' + ')[2]
                df['counter'] = value
                df['date'] = current_date
                df_sources = df_sources.append(df, ignore_index=True)
                print(key, value, 'success')
            except:
                print(key, value, 'problem')
                print(data)

    return df_sources

def load():
    df_sources=get_info_from_api()
    df_sources = df_sources.fillna(0).set_index('date')

    pulse_clickhouse.put_df_to_ch('top_refservers_replicated_distributed',df_sources)
