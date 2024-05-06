import functions_evm
import pandas as pd

# Import list of addresses
address_list = pd.read_csv('registry_address_lifi.csv')
address_list = address_list.rename(columns={'EVM Address': 'address'})
addresses = address_list['address']

days = 1
API_KEY = 'XX' # Debank API key

# Get EVM Activity
df = functions_evm.get_daily_activity(addresses, API_KEY, days )

#df.to_csv('ACCESS FILE')
