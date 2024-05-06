import requests
import pandas as pd
import datetime
import json
from datetime import datetime, timedelta


def get_solana_activity(address, API_KEY_SOLANA):

    url = 'https://portfolio-api.sonar.watch/v1/portfolio/fetch'
    headers = {
        'accept': 'application/json',
        'Authorization': API_KEY_SOLANA
    }
    params = {
        'useCache': 'false',
        'returnTokenInfo': 'false',
        'address': address,
        'addressSystem': 'solana'
    }

    response = requests.get(url, params=params, headers=headers)
    data = response.json()
    df = pd.DataFrame(data['elements'])
    
    df['address'] = address
    
    df = df[['address','label','platformId', 'value']]
    
    df = df.rename(columns={'label': 'cat', 'platformId': 'subcat'})
    
    return df


def get_jumper_activity(address,days):
    
    url = f"https://li.quest/v1/analytics/transfers?integrator=jumper.exchange&wallet={address}"
    headers = {"accept": "application/json"}
    response = requests.get(url, headers=headers)
    
    time = int((datetime.now() - timedelta(days=days)).timestamp())
    
    data = response.json()
    df = pd.DataFrame(data['transfers'])
    df = pd.json_normalize(df['receiving'])

    # Check if the transfer occurred within the last timestamp
    filtered_df = df.loc[df['timestamp'] >= time]
    filtered_df['amountUSD'] = filtered_df['amountUSD'].astype(float)
    
    
    filtered_df = filtered_df.groupby(['chainId']).sum('amountUSD').reset_index()
    filtered_df['cat'] = 'jumper_activity'
    filtered_df = filtered_df.rename(columns={'chainId': 'subcat', 'amountUSD': 'value'})
    filtered_df['address'] = address
    
    filtered_df = filtered_df[['address','value','cat','subcat']]
    
    
    return filtered_df

def get_solana_wallet_activity(address, days, API_KEY_SOLANA):
    c= get_solana_activity(address, API_KEY_SOLANA)
    f =  get_jumper_activity(address,days)
    
    if f.empty:
        df = c
    else:
        df = pd.concat([c, f])
    
    df['date'] = datetime.today().strftime('%Y-%m-%d')
    
    return df

def get_solana_daily_activity(address_list, API_KEY_SOLANA, days):
  final_df = pd.DataFrame()
  for address in address_list:
    print(address)
    df = get_wallet_activity(address,days, API_KEY_SOLANA)
    final_df = pd.concat([df, final_df])
  return final_df
