
# coding: utf-8

# In[4]:

import json
import requests
import pandas as pd
import time
import numpy as np
import os
import re
from tqdm import tqdm
import datetime
from send_email import email_sender

#=====定义函数====
from HTMLTable import HTMLTable
'''
生成html表格
传入一个dataframe, 设置一个标题， 返回一个html格式的表格
'''
def create_html_table(df, title):
    table = HTMLTable(caption=title)

    # 表头行
    table.append_header_rows((tuple(df.columns),))

    # 数据行
    for i in range(len(df.index)):
        table.append_data_rows((
            tuple(df.iloc[df.index[i],]),
        ))

    # 标题样式
    table.caption.set_style({
        'font-size': '15px',
    })

    # 表格样式，即<table>标签样式
    table.set_style({
        'border-collapse': 'collapse',
        'word-break': 'keep-all',
        'white-space': 'nowrap',
        'font-size': '14px',
    })

    # 统一设置所有单元格样式，<td>或<th>
    table.set_cell_style({
        'border-color': '#000',
        'border-width': '1px',
        'border-style': 'solid',
        'padding': '5px',
        'text-align': 'center',
    })

    # 表头样式
    table.set_header_row_style({
        'color': '#fff',
        'background-color': '#48a6fb',
        'font-size': '15px',
    })

    # 覆盖表头单元格字体样式
    table.set_header_cell_style({
        'padding': '15px',
    })

    # 调小次表头字体大小
    table[0].set_cell_style({
        'padding': '8px',
        'font-size': '15px',
    })

    html_table = table.to_html()
    return html_table

# ======= 正式开始执行
niu_start_date = '2022-09-30'

url_address = [ 'https://api.glassnode.com/v1/metrics/market/price_usd_ohlc']
url_name = ['k_fold']
# insert your API key here
API_KEY = '26BLocpWTcSU7sgqDdKzMHMpJDm'
data_list = []
for num in range(len(url_name)):
    print(num)
    addr = url_address[num]
    name = url_name[num]
    # make API request
    res_addr = requests.get(addr,params={'a': 'BTC', 'api_key': API_KEY})
    # convert to pandas dataframe
    ins = pd.read_json(res_addr.text, convert_dates=['t'])
    #ins.to_csv('test.csv')
    #print(ins['o'])
    ins['date'] =  ins['t']
    ins['value'] =  ins['o']
    ins = ins[['date','value']]
    data_list.append(ins)
result_data = data_list[0][['date']]
for i in range(len(data_list)):
    df = data_list[i]
    result_data = result_data.merge(df,how='left',on='date')
#last_data = result_data[(result_data.date>='2016-01-01') & (result_data.date<='2020-01-01')]
last_data = result_data[(result_data.date>=niu_start_date)]
last_data = last_data.sort_values(by=['date'])
last_data = last_data.reset_index(drop=True)
#print(type(last_data))
date = []
open_p = []
close_p = []
high_p = []
low_p = []
for i in range(len(last_data)):
    date.append(last_data['date'][i])
    open_p.append(last_data['value'][i]['o'])
    close_p.append(last_data['value'][i]['c'])
    high_p.append(last_data['value'][i]['h'])
    low_p.append(last_data['value'][i]['l'])
res_data = pd.DataFrame({'date':date,'open':open_p,'close':close_p,'high':high_p,'low':low_p})
combine_data = res_data
# 引入永续合约流动性的概念
url_address = ['https://api.glassnode.com/v1/metrics/derivatives/futures_liquidated_volume_long_relative',
              'https://api.glassnode.com/v1/metrics/market/price_usd_close']
url_name = ['futures','price']
# insert your API key here
API_KEY = '26BLocpWTcSU7sgqDdKzMHMpJDm'
data_list = []
for num in range(len(url_name)):
    print(num)
    addr = url_address[num]
    name = url_name[num]
    # make API request
    res_addr = requests.get(addr,params={'a': 'BTC', 'api_key': API_KEY})
    # convert to pandas dataframe
    ins = pd.read_json(res_addr.text, convert_dates=['t'])
    #ins.to_csv('test.csv')
    #print(ins['o'])
    #print(ins)
    ins['date'] =  ins['t']
    ins['value'] =  ins['v']
    ins = ins[['date','value']]
    data_list.append(ins)
result_data = data_list[0][['date']]
for i in range(len(data_list)):
    df = data_list[i]
    result_data = result_data.merge(df,how='left',on='date')
#last_data = result_data[(result_data.date>='2016-01-01') & (result_data.date<='2020-01-01')]
futures_data = result_data[(result_data.date>='2013-01-01')]
futures_data = futures_data.sort_values(by=['date'])
futures_data = futures_data.reset_index(drop=True)
futures_data['next_value'] = futures_data['value_y'].shift(-1)
flag_1 = []
flag_2 = []
flag_3 = []
for i in range(len(futures_data)):
    if futures_data['next_value'][i] > futures_data['value_y'][i]:
        flag_1.append(1)
    else:
        flag_1.append(0)
    if futures_data['value_x'][i] > 0.5:
        flag_2.append(1)
    else:
        flag_2.append(0)
futures_data['flag_1'] = flag_1
futures_data['flag_2'] = flag_2
for i in range(len(futures_data)):
    if futures_data['flag_1'][i] == futures_data['flag_2'][i]:
        flag_3.append(1)
    else:
        flag_3.append(0)
futures_data['flag_3'] = flag_3
futures_value_1 = futures_data['value_x'][len(futures_data)-1]
futures_value_2 = futures_data['value_x'][len(futures_data)-2]
futures_value_3 = futures_data['value_x'][len(futures_data)-3]
futures_accury_2 = futures_data['flag_3'][len(futures_data)-2]
futures_accury_3 = futures_data['flag_3'][len(futures_data)-3]
if futures_value_1 > 0.5 and futures_value_2 > 0.8 and futures_value_3 > 0.8 and futures_accury_2 == 0 and futures_accury_3 == 0:
    pingjia = 'duotou_finish_kill'
elif futures_value_1 > 0.8 and futures_value_2 > 0.8 and futures_accury_2 == 0:
    pingjia = 'duotou_ing_kill'
elif futures_value_1 > 0.8 and futures_value_2 < 0.5 and futures_accury_2 == 1:
    pingjia = 'duotou_start_kill'
elif futures_value_1 < 0.5 and futures_value_2 < 0.2 and futures_value_3 < 0.2 and futures_accury_2 == 0 and futures_accury_3 == 0:
    pingjia = 'kongtou_finish_kill'
elif futures_value_1 < 0.2 and futures_value_2 < 0.2 and futures_accury_2 == 0:
    pingjia = 'kongtou_ing_kill'
elif futures_value_1 < 0.2 and futures_value_2 > 0.5 and futures_accury_2 == 1:
    pingjia = 'kongtou_start_kill'
elif futures_value_1 > 0.5:
    pingjia = 'duotou_main'
elif futures_value_1 < 0.5:
    pingjia = 'kongtou_main'
else:
    pingjia = 'unknow_reason'
# 价格不能在最低点
# 价格大于过去两天
# 价格超过5日均线值
last_price = res_data['close'][len(res_data)-1]
if last_price == np.max(res_data['close']):
    zuigao = 1
else:
    zuigao = 0
high_price = np.max(res_data['close'][0:len(res_data)-1]) * 0.95
two_min = np.min(res_data['close'][len(res_data)-3:len(res_data)-1])
mean_5_day = 0.985 * np.mean(res_data['close'][len(res_data)-6:len(res_data)-1])

if zuigao == 1:
    last_value = 9
elif last_price > high_price:
    last_value = 1
elif last_price < high_price and (last_price < two_min or last_price > mean_5_day):
    last_value = 1
else:
    last_value = 0
date_value = res_data['date'][len(res_data)-1] + datetime.timedelta(days=1)

def judge_label1():
    url_address = [ 'https://api.glassnode.com/v1/metrics/market/price_usd_ohlc']
    url_name = ['k_fold']
    # insert your API key here
    API_KEY = '26BLocpWTcSU7sgqDdKzMHMpJDm'
    data_list = []
    for num in range(len(url_name)):
        print(num)
        addr = url_address[num]
        name = url_name[num]
        # make API request
        res_addr = requests.get(addr,params={'a': 'BTC', 'api_key': API_KEY})
        # convert to pandas dataframe
        ins = pd.read_json(res_addr.text, convert_dates=['t'])
        #ins.to_csv('test.csv')
        #print(ins['o'])
        ins['date'] =  ins['t']
        ins['value'] =  ins['o']
        ins = ins[['date','value']]
        data_list.append(ins)
    result_data = data_list[0][['date']]
    for i in range(len(data_list)):
        df = data_list[i]
        result_data = result_data.merge(df,how='left',on='date')
    #last_data = result_data[(result_data.date>='2016-01-01') & (result_data.date<='2020-01-01')]
    last_data = result_data[(result_data.date>=niu_start_date)]
    last_data = last_data.sort_values(by=['date'])
    last_data = last_data.reset_index(drop=True)
    #print(type(last_data))
    date = []
    open_p = []
    close_p = []
    high_p = []
    low_p = []
    for i in range(len(last_data)):
        date.append(last_data['date'][i])
        open_p.append(last_data['value'][i]['o'])
        close_p.append(last_data['value'][i]['c'])
        high_p.append(last_data['value'][i]['h'])
        low_p.append(last_data['value'][i]['l'])
    res_data = pd.DataFrame({'date':date,'open':open_p,'close':close_p,'high':high_p,'low':low_p})
    #res_data['judge'] = res_data['date'].apply(lambda x:cal(x))
    #res_data = res_data[res_data.judge == num_list]
    #res_data = res_data[res_data.judge==1]
    res_data = res_data.sort_values(by=['date'])
    res_data = res_data.reset_index(drop=True)    
    res = pd.DataFrame()
    for i in range(30,len(res_data)):
        date_j = res_data['date'][i]
        open_j = res_data['open'][i]
        close_j = res_data['close'][i]
        high_j = res_data['high'][i]
        low_j = res_data['low'][i]
        per = (res_data['high'][i]-res_data['open'][i])/res_data['open'][i]
        ins = res_data[(res_data['date']<date_j)]
        #print(np.min(ins['open']),open_j,(open_j - np.min(ins['open']))/np.min(ins['open']))
        ele = list(res_data['close'][i-2:i])

        if (close_j - np.max(ins['close']))/np.max(ins['close']) < -0.05:
            judge_duo = 1
        else:
            judge_duo = 0

        if close_j - np.min(ele) < 0:
            judge_l3_min = 1
        else:
            judge_l3_min = 0 

        if close_j >= np.max(ins['close']):
            judge_zuigao = 1
        else:
            judge_zuigao = 0

        dat = pd.DataFrame({'date':date_j,'open':open_j,'close':close_j,'high':high_j,'low':low_j,'judge_l3_min':judge_l3_min,'judge_his':judge_duo,'judge_zuigao':judge_zuigao},index=[0])
        res = pd.concat([res,dat])
    return res
def judge_label2():
    url_address = [ 'https://api.glassnode.com/v1/metrics/market/price_usd_ohlc']
    url_name = ['k_fold']
    # insert your API key here
    API_KEY = '26BLocpWTcSU7sgqDdKzMHMpJDm'
    data_list = []
    for num in range(len(url_name)):
        print(num)
        addr = url_address[num]
        name = url_name[num]
        # make API request
        res_addr = requests.get(addr,params={'a': 'BTC', 'api_key': API_KEY})
        # convert to pandas dataframe
        ins = pd.read_json(res_addr.text, convert_dates=['t'])
        #ins.to_csv('test.csv')
        #print(ins['o'])
        ins['date'] =  ins['t']
        ins['value'] =  ins['o']
        ins = ins[['date','value']]
        data_list.append(ins)
    result_data = data_list[0][['date']]
    for i in range(len(data_list)):
        df = data_list[i]
        result_data = result_data.merge(df,how='left',on='date')
    #last_data = result_data[(result_data.date>='2016-01-01') & (result_data.date<='2020-01-01')]
    last_data = result_data[(result_data.date>='2013-01-01')]
    last_data = last_data.sort_values(by=['date'])
    last_data = last_data.reset_index(drop=True)
    print(type(last_data))
    date = []
    open_p = []
    close_p = []
    high_p = []
    low_p = []
    for i in range(len(last_data)):
        date.append(last_data['date'][i])
        open_p.append(last_data['value'][i]['o'])
        close_p.append(last_data['value'][i]['c'])
        high_p.append(last_data['value'][i]['h'])
        low_p.append(last_data['value'][i]['l'])
    res_data = pd.DataFrame({'date':date,'open':open_p,'close':close_p,'high':high_p,'low':low_p})
    #查看收盘价是否站上了5日均线
    res_data = res_data.sort_values(by='date')
    res_data = res_data.reset_index(drop=True)
    date_1 = []
    open_1 = []
    close_1 = []
    high_1 = []
    low_1 = []
    ma5 = []
    for i in range(5,len(res_data)):
        ins = res_data[i-5:i]
        ins = ins.sort_values(by='date')
        ins = ins.reset_index(drop=True)
        per = (res_data['close'][i] - np.mean(ins['close']))/np.mean(ins['close'])
        #if (ins['close'][4] - np.mean(ins['open']))/np.mean(ins['open']) > -0.03:
        if per > -0.015:
            ma5.append(1)
        else:
            ma5.append(0)
        date_1.append(res_data['date'][i])
        open_1.append(res_data['open'][i])
        close_1.append(res_data['close'][i])
        high_1.append(res_data['high'][i])
        low_1.append(res_data['low'][i])    
    result_data = pd.DataFrame({'date':date_1,'ma5_duo':ma5})
    return result_data

label1 = judge_label1()
label2 = judge_label2()
last_label = label1.merge(label2,how='left',on=['date'])
last_df = last_label.sort_values(by='date')
last_df = last_df.reset_index(drop=True)
last_df = last_df.dropna()
last_df = last_df.reset_index(drop=True)
print(last_df)
#探索价格波动
url_address = [ 'https://api.glassnode.com/v1/metrics/market/price_usd_close',
                'https://api.glassnode.com/v1/metrics/transactions/count']
url_name = ['price','count']
# insert your API key here
API_KEY = '26BLocpWTcSU7sgqDdKzMHMpJDm'
data_list = []
for num in range(len(url_name)):
    #print(num)
    addr = url_address[num]
    name = url_name[num]
    # make API request
    res_addr = requests.get(addr,params={'a': 'BTC', 'i':'1h','api_key': API_KEY})
    # convert to pandas dataframe
    ins = pd.read_json(res_addr.text, convert_dates=['t'])
    #ins.to_csv('test.csv')
    #print(ins['o'])
    ins['date'] =  ins['t']
    ins[name] =  ins['v']
    ins = ins[['date',name]]
    data_list.append(ins)
result_data = data_list[0][['date']]
for i in range(len(data_list)):
    df = data_list[i]
    result_data = result_data.merge(df,how='left',on='date')
result_data = data_list[0][['date']]
for i in range(len(data_list)):
    df = data_list[i]
    result_data = result_data.merge(df,how='left',on='date')
#last_data = result_data[(result_data.date>='2016-01-01') & (result_data.date<='2020-01-01')]
last_data = result_data[(result_data.date>='2013-01-01')]
last_data['new_date'] = last_data['date'].map(lambda x: str(x)[0:10])
last_data['new_date'] = pd.to_datetime(last_data['new_date'])
last_data['hour'] = last_data['date'].map(lambda x: x.hour)
last_data = last_data.sort_values(by=['new_date','hour'])
sub_last_data = last_data[last_data.hour==0]
sub_last_data['new_date'] = sub_last_data['new_date'] - datetime.timedelta(days=1)
sub_last_data['hour'] = 24
last_data = pd.concat([last_data,sub_last_data])
last_data = last_data.sort_values(by=['new_date','hour'])
#将小时数据进行最高值和最低值的替换
date_list = sorted(list(set(last_df['date'])))
res_data = pd.DataFrame()
for time in tqdm(date_list):
    sub_last_data = last_data[last_data.new_date==time]
    sub_last_df = last_df[last_df.date==time]
    sub_last_data = sub_last_data.sort_values(by=['price','hour'])
    sub_last_data = sub_last_data.reset_index(drop=True)
    sub_last_df = sub_last_df.reset_index(drop=True)

    sub_last_data['price'][24]  =  sub_last_df['high'][0]
    sub_last_data['price'][0] =  sub_last_df['low'][0]

    res_data = pd.concat([res_data,sub_last_data])
res_data = res_data.sort_values(by=['new_date','hour'])
res_data = res_data.reset_index(drop=True)

res_data['price_1'] = res_data['price'].shift(-1) 
res_data['price_2'] = res_data['price'].shift(-2) 
res_data['price_3'] = res_data['price'].shift(-3) 
res_data['price_4'] = res_data['price'].shift(-4) 
res_data['price_5'] = res_data['price'].shift(-5) 
res_data['price_6'] = res_data['price'].shift(-6) 
res_data['price_7'] = res_data['price'].shift(-7) 
res_data['price_8'] = res_data['price'].shift(-8) 
def xiuzheng(x):
    if x[1] < 10 and x[5]>300:
        y = x[5]
    elif x[1] < 10 and x[5]<10 and x[6]>300:
        y = x[6]  
    elif x[1] < 10 and x[5]<10 and x[6]<10 and x[7]>300:
        y = x[7]  
    elif x[1] < 10 and x[5]<10 and x[6]<10 and x[7]<10 and x[8]>300:
        y = x[8]  
    else:
        y = x[1]
    return y 
res_data['price_xiu'] = res_data.apply(lambda x:xiuzheng(x),axis=1)

date_s = []
date_e = []
open_p = []
close_p = []
zuigao_p = []
res = []
per = []
i = 0
while i < len(last_df)-1:
    #print(i)
    judge_his = last_df['judge_his'][i]
    ma5_duo = last_df['ma5_duo'][i]
    judge_l3_min = last_df['judge_l3_min'][i]
    if (judge_his == 1 and  (judge_l3_min == 1 or  ma5_duo == 1)) or judge_his ==0:
        open_price = last_df['open'][i+1]
        start_date = last_df['date'][i+1]
        print(start_date)
        open_p.append(open_price)
        date_s.append(start_date)
        zuigao_p.append(last_df['judge_zuigao'][i])
        sub_later_data = res_data[res_data.new_date>=start_date]
        sub_later_data = sub_later_data.sort_values(by=['new_date','hour'])
        sub_later_data = sub_later_data.reset_index(drop=True)
        for j in range(len(sub_later_data)):
            #print('====' +str(j))
            close_price = sub_later_data['price_xiu'][j]
            close_date = sub_later_data['new_date'][j]
            if (close_price - open_price)/open_price >= 0.09:
                close_p.append(close_price)
                date_e.append(close_date)
                res.append(1)
                per.append(0.09)

                find_i = last_df[last_df.date==close_date]
                i = find_i.index[0]
                break
            elif (close_price - open_price)/open_price <= -0.05:
                close_p.append(close_price)
                date_e.append(close_date)
                res.append(0)
                per.append(-0.05)

                find_i = last_df[last_df.date==close_date]
                i = find_i.index[0]
                break
            elif (close_price - open_price)/open_price > 0.03 and (close_price - open_price)/open_price < 0.09:
                sub_later_data['label'] = sub_later_data.index
                sub_later_data_1 = sub_later_data[sub_later_data.label > j]
                sub_later_data_1 = sub_later_data_1.sort_values(by=['new_date','hour'])
                sub_later_data_1 = sub_later_data_1.reset_index(drop=True)
                for w in range(len(sub_later_data_1)):
                    #print(w)
                    close_price = sub_later_data_1['price_xiu'][w]
                    close_date = sub_later_data_1['new_date'][w]
                    if (close_price - open_price)/open_price <= 0.03:
                        close_p.append(open_price*1.03)
                        date_e.append(close_date)
                        res.append(1)
                        per.append(0.03)

                        find_i = last_df[last_df.date==close_date]
                        i = find_i.index[0]
                        #i += 3
                        break
                    elif (close_price - open_price)/open_price >= 0.09:
                        close_p.append(close_price*1.09)
                        date_e.append(close_date)
                        res.append(1)
                        per.append(0.09)

                        find_i = last_df[last_df.date==close_date]
                        i = find_i.index[0]
                        break
                    else:
                        if w == len(sub_later_data_1)-1:
                            i += 100000000
                            break
                        else:
                            continue
                break
            else:
                if j == len(sub_later_data)-1:
                    i += 100000000
                    break
                else:
                    continue
    else:
        i += 1

if len(date_e) < len(date_s):
    date_e.append('2099-12-31')
    close_p.append(999999999)
    per.append(0)
    res.append(0)
res_df = pd.DataFrame({'date_s':date_s,'date_e':date_e,'open_p':open_p,'close_p':close_p,'per':per,'res':res,'zuigao':zuigao_p})

#======自动发邮件
content = create_html_table(res_df, f'BTC判断日期{date_value}')
#设置服务器所需信息
#163邮箱服务器地址
mail_host = 'smtp.163.com'  
#163用户名
mail_user = 'lee_daowei@163.com'  
#密码(部分邮箱为授权码) 
mail_pass = 'GKXGKVGTYBGRMAVE'   
#邮件发送方邮箱地址
sender = 'lee_daowei@163.com'  

#邮件接受方邮箱地址，注意需要[]包裹，这意味着你可以写多个邮件地址群发
receivers = ['lee_daowei@163.com']  
context = f'每日回顾---BTC做多日期判断{date_value}'
email_sender(mail_host,mail_user,mail_pass,sender,receivers,context,content)
