
import requests
import pandas as pd
import datetime
import json
from datetime import datetime, timedelta



address_list = pd.read_csv('registry_address_lifi.csv')    

def get_protocols(address, API_KEY): 
    url = f"https://pro-openapi.debank.com/v1/user/all_simple_protocol_list?id={address}&chain_ids=blast,scrl,era,metis,linea,base,eth,op,arb,xdai,matic,bsc,avax"
    headers = {
    'accept': 'application/json',
    'AccessKey': APY_KEY  # Replace 'API_KEY' with your actual access key
    }
    response = requests.get(url, headers=headers)
    data = response.json()
    
    if data == []:
        df = pd.DataFrame(columns=['chain', 'name', 'net_usd_value', 'asset_usd_value', 'address'])
    else:
        df = pd.DataFrame(data)
        df['address'] = address
        
        # keep only "significant positions"
        df = df[df['asset_usd_value'] > 10]
        
        df['cat'] = 'protocols'
        
        df = df[['address','net_usd_value','cat','name']]
        df = df.groupby(['address', 'cat','name']).sum('net_usd_value').reset_index()
        
        df = df.rename(columns={'name': 'subcat', 'net_usd_value': 'value'})
    
    return df

#c= get_protocols('0xb29601eB52a052042FB6c68C69a442BD0AE90082')


## 30 units
def get_balance(address, API_KEY): 
    url = f"https://pro-openapi.debank.com/v1/user/total_balance?id={address}"
    headers = {
    'accept': 'application/json',
    'AccessKey': API_KEY
    }
    response = requests.get(url, headers=headers)
    data = response.json()
    
    df = pd.DataFrame(data)
    df = df.head(1)
    
    df['address'] = address
    df['cat'] = 'balance'
    df['subcat'] = 'NA'
    
    df = df[['address','cat','subcat', 'total_usd_value']]
    
    df = df.rename(columns={'total_usd_value': 'value'})
    
    return df

#d = get_balance('0xb29601eB52a052042FB6c68C69a442BD0AE90082')


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

#f =  get_jumper_activity('0xb29601eB52a052042FB6c68C69a442BD0AE90082',80)

def get_wallet_activity(address, days, API_KEY):
    c= get_protocols(address, API_KEY)
    d = get_balance(address, API_KEY)
    f =  get_jumper_activity(address,days)
    
    df = pd.concat([c, d, f])
    
    df['date'] = datetime.today().strftime('%Y-%m-%d')
    
    return df


#final = get_wallet_activity('0xb29601eB52a052042FB6c68C69a442BD0AE90082',2)


def get_daily_activity(address_list, days = 1, API_KEY):
  final_df = pd.DataFrame()
  for address in address_list['address']:
    df = get_wallet_activity('0xb29601eB52a052042FB6c68C69a442BD0AE90082',1)
    final_df = pd.concat(df, final_df)
return final_df
