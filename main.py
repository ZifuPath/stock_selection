from py5paisa import FivePaisaClient
from dotenv import load_dotenv
import requests
import pandas as pd
import ast
import os
import datetime
import time
load_dotenv()

def get_future_scrips():
    # GET FUTURE STOCKS FROM 5PAISA SCRIPS
    df = pd.read_csv('dataset/scripmaster-csv-format.csv',usecols=['Exch', 'ExchType', 'Scripcode', 'Name', 'Series', 'Expiry',  'Root'])
    df1 = df[(df.Exch=='N') & (df.ExchType=='D')]
    roots = list(df1.Root.unique())
    df2 = df[(df.Exch == 'N') & (df.Series == 'EQ') & (df.Root.isin(roots))]
    df2 = df2[['Exch', 'ExchType', 'Scripcode','Root']]
    return df2

def get_client():
    #GET PY5PAISA OBJECT  return py5paisa object
    cred = ast.literal_eval(os.getenv('cred'))
    email = os.getenv('email')
    dob = os.getenv('dob')
    password = os.getenv('passwd')
    client = FivePaisaClient(email=email, passwd=password, dob=dob, cred=cred)
    client.login()
    return client

def calculate_CPR(df:pd.DataFrame):
    """ FUNCTION FOR THE CALCULATING CPR return dataframe with cpr"""
    df['Pivot'] = (df['Close'] + df['Low'] +df['High']).div(3)
    df['BC'] = (df['Low'] + df['High']).div(2)
    df['TC'] = (df['Pivot'] - df['BC'] + df['Pivot'])
    df['R1'] = 2 * df.Pivot - df.Low
    df['S1'] = 2 * df.Pivot - df.High
    df['R2'] = (df.Pivot-df.S1)  + df.R1
    df['S2'] = df.Pivot+df.S1  - df.R1
    df['R3'] = (df.Pivot-df.S2)  + df.R1
    df['S3'] = df.Pivot+df.S2  - df.R1
    df = df.round(2)
    return df

def get_pivot(df:pd.DataFrame,name:str):
    # CALCULATE CPR IN DATAFRAME AND RETURN df:pd.DataFrame
    df = calculate_CPR(df)
    df.loc[:,'Stock'] = name
    return df.tail(1)

def telegram_bot_sendtext(bot_message:str):
    #SEND MESSAGE THROUGH TELEGRAM
    bot_token = os.getenv('bot_token')
    bot_chatID = os.getenv('bot_chatID')
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message
    response = requests.get(send_text)
    print(bot_message, response)
    return response.json()

def get_pivots_next_day():
    # get CPR and support resistance for next day
    client = get_client()
    t1 = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    t2 = (datetime.datetime.today() - datetime.timedelta(days=10)).strftime('%Y-%m-%d')
    df = get_future_scrips()
    adict = df.to_dict('records')
    alist = []
    for stock in adict:
        try:
            df =client.historical_data(Exch=stock['Exch'],
                                   ExchangeSegment=stock['ExchType'],
                                   ScripCode=stock['Scripcode'],
                                   time='1d',From=t2,To=t1)

            df = get_pivot(df,stock['Root'])
            alist.append(df)
        except:
            pass
    df = pd.concat(alist,ignore_index=True)
    df = df.reset_index()
    df.to_csv('dataset/pivots.csv',index=False)


def buy_sell_signals():
    client = get_client()
    t1 = datetime.datetime.today().strftime('%Y-%m-%d')
    t3 = (datetime.datetime.today() - datetime.timedelta(days=0)).strftime('%Y-%m-%d')
    df = get_future_scrips()
    adict = df.to_dict('records')
    df_pivot = pd.read_csv('dataset/pivots.csv')
    for stock in adict:
        try:
            pivot = df_pivot[df_pivot['Stock']==stock['Root']]
            pivot = pivot.reset_index()
            pivot = pivot.to_dict('records')[0]
            df_daily =client.historical_data(Exch=stock['Exch'],
                                   ExchangeSegment=stock['ExchType'],
                                   ScripCode=stock['Scripcode'],
                                   time='5m',From=t3,To=t1)
            cond1 = [pivot['R1'] >=df_daily.Open.iloc[0]>=pivot['Pivot'],pivot['R2']>=df_daily.Close.iloc[0]>=pivot['R1']]
            cond2 = [pivot['S1'] <= df_daily.Open.iloc[0] <= pivot['Pivot'], pivot['S2'] <= df_daily.Close.iloc[0] <= pivot['S1']]
            if all(cond1):
                msg = f"{stock['Root']} : BUY ABOVE {df_daily.Close.iloc[0]} SL {df_daily.Open.iloc[0]}"
                telegram_bot_sendtext(msg)
            if all(cond2):
                msg = f"{stock['Root']} : SELL BELOW {df_daily.Close.iloc[0]} SL {df_daily.Open.iloc[0]}"
                telegram_bot_sendtext(msg)
            time.sleep(0.2)
        except BaseException as e:
            print(e.args)

if __name__ == '__main__':
    while datetime.datetime.now().time() < datetime.time(8, 50):
        get_pivots_next_day()
    while datetime.datetime.now().time()< datetime.time(9,20,30):
        time.sleep(1)
    buy_sell_signals()


