import functions_evm
import functions_solana
import pandas as pd

# Import list of addresses
address_list = pd.read_csv('registry_address_lifi.csv')
address_list = address_list.rename(columns={'EVM Address': 'address'})
address_list_solana = address_list.rename(columns={'Solana Address': 'sol_address'})
addresses = address_list['address']
sol_addresses = address_list['sol_address']

days = 1
API_KEY = 'XX' # Debank API key
API_KEY_SOLANA = 'XX' # Sonar Watch API key

# Get EVM Activity
df = functions_evm.get_daily_activity(addresses, API_KEY, days)

#Get Solana Activity
df_solana = functions_solana.get_solana_daily_activity(sol_addresses, API_KEY_SOLANA, days)

#df.to_csv('ACCESS FILE')
