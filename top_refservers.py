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
              'media_ediniy + desktop + yandex': '2957928',

              'media_ediniy + mobile + total': '283',
              'media_ediniy + mobile + main page': '3230026',
              'media_ediniy + mobile + pulse': '3230028',
              'media_ediniy + mobile + google': '2957446',
              'media_ediniy + mobile + yandex': '2957445',

              'media_movies + desktop + total': '2664990',
              'media_movies + desktop + main page': '3229915',
              'media_movies + desktop + pulse': '3229931',
              'media_movies + desktop + google': '2836119',
              'media_movies + desktop + yandex': '2836120',

              'media_movies + mobile + total': '2665053',
              'media_movies + mobile + main page': '3229937',
              'media_movies + mobile + pulse': '3229941',
              'media_movies + mobile + google': '2836153',
              'media_movies + mobile + yandex': '2836154',

              'media_auto + desktop + total': '2664992',
              'media_auto + desktop + main page': '3230152',
              'media_auto + desktop + pulse': '3229959',
              'media_auto + desktop + google': '2836121',
              'media_auto + desktop + yandex': '2836122',

              'media_auto + mobile + total': '2665054',
              'media_auto + mobile + main page': '3230154',
              'media_auto + mobile + pulse': '3229963',
              'media_auto + mobile + google': '2836155',
              'media_auto + mobile + yandex': '2836156',

              'media_lady + desktop + total': '2664997',
              'media_lady + desktop + main page': '3229973',
              'media_lady + desktop + pulse': '3229975',
              'media_lady + desktop + google': '2836131',
              'media_lady + desktop + yandex': '2836132',

              'media_lady + mobile + total': '2665026',
              'media_lady + mobile + main page': '3229979',
              'media_lady + mobile + pulse': '3229983',
              'media_lady + mobile + google': '2836148',
              'media_lady + mobile + yandex': '2836149',

              'media_news + desktop + total': '2642541',
              'media_news + desktop + main page': '3229401',
              'media_news + desktop + pulse': '3229404',
              'media_news + desktop + google': '2836107',
              'media_news + desktop + yandex': '2836108',

              'media_news + mobile + total': '2360450',
              'media_news + mobile + main page': '3229999',
              'media_news + mobile + pulse': '3230001',
              'media_news + mobile + google': '2836086',
              'media_news + mobile + yandex': '2836087',

              'media_sport + desktop + total': '2642543',
              'media_sport + desktop + main page': '3230003',
              'media_sport + desktop + pulse': '3230005',
              'media_sport + desktop + google': '2836109',
              'media_sport + desktop + yandex': '2836110',

              'media_sport + mobile + total': '2360452',
              'media_sport + mobile + main page': '3230011',
              'media_sport + mobile + pulse': '3230008',
              'media_sport + mobile + google': '2836088',
              'media_sport + mobile + yandex': '2836089',

              'media_kids + desktop + total': '2665015',
              'media_kids + desktop + main page': '3230013',
              'media_kids + desktop + pulse': '3230015',
              'media_kids + desktop + google': '2836138',
              'media_kids + desktop + yandex': '2836139',

              'media_kids + mobile + total': '2665060',
              'media_kids + mobile + main page': '3230017',
              'media_kids + mobile + pulse': '3230019',
              'media_kids + mobile + google': '2836157',
              'media_kids + mobile + yandex': '2836158',

              'media_health + desktop + total': '2665016',
              'media_health + desktop + main page': '3230156',
              'media_health + desktop + pulse': '3229985',
              'media_health + desktop + google': '2836140',
              'media_health + desktop + yandex': '2836141',

              'media_health + mobile + total': '2360469',
              'media_health + mobile + main page': '3230158',
              'media_health + mobile + pulse': '3229987',
              'media_health + mobile + google': '2836095',
              'media_health + mobile + yandex': '2836096',

              'media_hi_tech + desktop + total': '2665017',
              'media_hi_tech + desktop + main page': '3229965',
              'media_hi_tech + desktop + pulse': '3229967',
              'media_hi_tech + desktop + google': '2836142',
              'media_hi_tech + desktop + yandex': '2836143',

              'media_hi_tech + mobile + total': '2665061',
              'media_hi_tech + mobile + main page': '3229969',
              'media_hi_tech + mobile + pulse': '3229971',
              'media_hi_tech + mobile + google': '2836159',
              'media_hi_tech + mobile + yandex': '2836160',

              'media_weather + desktop + total': '2665019',
              'media_weather + desktop + main page': '3229939',
              'media_weather + desktop + pulse': '3229943',
              'media_weather + desktop + google': '2836144',
              'media_weather + desktop + yandex': '2836145',

              'media_weather + mobile + total': '2360465',
              'media_weather + mobile + main page': '3229949',
              'media_weather + mobile + pulse': '3229951',
              'media_weather + mobile + google': '2836090',
              'media_weather + mobile + yandex': '2836091',

              'media_pets + desktop + total': '2958948',
              'media_pets + desktop + main page': '3229917',
              'media_pets + desktop + pulse': '3229919',
              'media_pets + desktop + google': '3229927',
              'media_pets + desktop + yandex': '3229929',

              'media_pets + mobile + total': '2958958',
              'media_pets + mobile + main page': '3229921',
              'media_pets + mobile + pulse': '3229924',
              'media_pets + mobile + google': '3229933',
              'media_pets + mobile + yandex': '3229935',

              'news_android_app + mobile + total': '3228893',
              'pulse + total + total': '3079979',
              'dobro + total + total':'2370352',
              'pulse_fxt + total + total':'3163270',
              'hichef + total + total':'3152105',
              'vseapteki + total + total':'2953536',
              'goro + total + total':'560135',
              'Hi-Chef.ru + total + total':'3127436'

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
