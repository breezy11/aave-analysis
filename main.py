import json
import csv
import pandas as pd
from datetime import datetime
from os import listdir
from os.path import isfile, join

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

# 1. Imports

# import borrows data
with open('data/borrows.json') as json_file:
    borrows = json.load(json_file)
data_list = []
for x in borrows:
    data_list += list(list(x.values())[0].values())[0]
list_ = []
for x in data_list:
    list_.append([x['id'], x['pool']['id'], x['user']['id'], x['amount'], x['borrowRate'], x['borrowRateMode'], x['accruedBorrowInterest'], x['timestamp']])
borr = pd.DataFrame(list_, columns = ['borrow-id', 'pool-id', 'user-id', 'amount', 'borrow-rate', 'borrow-rate-mode', 'accrued-borrow-interest', 'timestamp'])
borr['date'] = borr['timestamp'].apply(lambda x: datetime.fromtimestamp(x))
borr.drop('timestamp', axis=1, inplace=True)

# import deposits data
with open('data/deposits.json') as json_file:
    deposits = json.load(json_file)
data_list = []
for x in deposits:
    data_list += list(list(x.values())[0].values())[0]
list_ = []
for x in data_list:
    list_.append([x['id'], x['pool']['id'], x['user']['id'], x['amount'], x['timestamp']])
dep = pd.DataFrame(list_, columns = ['deposit-id', 'pool-id', 'user-id', 'amount', 'timestamp'])
dep['date'] = dep['timestamp'].apply(lambda x: datetime.fromtimestamp(x))
dep.drop('timestamp', axis=1, inplace=True)

# import luqidation_calls data
with open('data/liquidation_calls.json') as json_file:
    liquidation = json.load(json_file)
data_list = []
for x in liquidation:
    data_list += list(list(x.values())[0].values())[0]
list_ = []
for x in data_list:
    list_.append([x['id'], x['pool']['id'], x['user']['id'], x['collateralAmount'], x['principalAmount'], x['liquidator'], x['timestamp']])
liq = pd.DataFrame(list_, columns = ['liquidation-call-id', 'pool-id', 'user-id', 'collateral-amount', 'principal-amount', 'liquidator', 'timestamp'])
liq['date'] = liq['timestamp'].apply(lambda x: datetime.fromtimestamp(x))
liq.drop('timestamp', axis=1, inplace=True)

# import origination_fee_liquidation data
with open('data/origination_fee_liquidation.json') as json_file:
    orig_fee_liq = json.load(json_file)
data_list = []
for x in orig_fee_liq:
    data_list += list(list(x.values())[0].values())[0]
list_ = []
for x in data_list:
    list_.append([x['id'], x['pool']['id'], x['user']['id'], x['feeLiquidated'], x['liquidatedCollateralForFee'], x['timestamp']])
orig = pd.DataFrame(list_, columns = ['origination-fee-liquidation-id', 'pool-id', 'user-id', 'fee-liquidated', 'liquidated-collateral-for-free', 'timestamp'])
orig['date'] = orig['timestamp'].apply(lambda x: datetime.fromtimestamp(x))
orig.drop('timestamp', axis=1, inplace=True)

# import rebalance_stable_borrow_rate data
with open('data/rebalance_stable_borrow_rate.json') as json_file:
    reb_stab_borr = json.load(json_file)
data_list = []
for x in reb_stab_borr:
    data_list += list(list(x.values())[0].values())[0]
list_ = []
for x in data_list:
    list_.append([x['id'], x['pool']['id'], x['user']['id'], x['borrowRateFrom'], x['borrowRateTo'], x['accruedBorrowInterest'], x['timestamp']])
reb = pd.DataFrame(list_, columns = ['reb-stable-borrow-rate-id', 'pool-id', 'user-id', 'borrow-rate-from', 'borrow-rate-to', 'accrued-borrow-interest','timestamp'])
reb['date'] = reb['timestamp'].apply(lambda x: datetime.fromtimestamp(x))
reb.drop('timestamp', axis=1, inplace=True)

# import redeem_underlyings data
with open('data/redeem_underlyings.json') as json_file:
    red_under = json.load(json_file)
data_list = []
for x in red_under:
    data_list += list(list(x.values())[0].values())[0]
list_ = []
for x in data_list:
    list_.append([x['id'], x['pool']['id'], x['user']['id'], x['amount'], x['timestamp']])
red = pd.DataFrame(list_, columns = ['redeem-underlying-id', 'pool-id', 'user-id', 'amount', 'timestamp'])
red['date'] = red['timestamp'].apply(lambda x: datetime.fromtimestamp(x))
red.drop('timestamp', axis=1, inplace=True)

# import repays data
with open('data/repays.json') as json_file:
    repays = json.load(json_file)
data_list = []
for x in repays:
    data_list += list(list(x.values())[0].values())[0]
list_ = []
for x in data_list:
    list_.append([x['id'], x['pool']['id'], x['user']['id'], x['amountAfterFee'], x['fee'], x['timestamp']])
rep = pd.DataFrame(list_, columns = ['repay-id', 'pool-id', 'user-id', 'amount-after-fee','fee', 'timestamp'])
rep['date'] = rep['timestamp'].apply(lambda x: datetime.fromtimestamp(x))
rep.drop('timestamp', axis=1, inplace=True)

# import swaps data
with open('data/swaps.json') as json_file:
    swaps = json.load(json_file)
data_list = []
for x in swaps:
    data_list += list(list(x.values())[0].values())[0]
list_ = []
for x in data_list:
    list_.append([x['id'], x['pool']['id'], x['user']['id'], x['borrowRateFrom'], x['borrowRateModeFrom'],x['borrowRateTo'], x['borrowRateModeTo'], x['accruedBorrowInterest'], x['timestamp']])
swap = pd.DataFrame(list_, columns = ['swap-id', 'pool-id', 'user-id', 'borrow-rate-from', 'borrow-rate-mode-from', 'borrow-rate-to', 'borrow-rate-mode-to', 'accrued-borrow-interest', 'timestamp'])
swap['date'] = swap['timestamp'].apply(lambda x: datetime.fromtimestamp(x))
swap.drop('timestamp', axis=1, inplace=True)

# import usage_as_collaterals data
with open('data/usage_as_collaterals.json') as json_file:
    usag_coll = json.load(json_file)
data_list = []
for x in usag_coll:
    data_list += list(list(x.values())[0].values())[0]
list_ = []
for x in data_list:
    list_.append([x['id'], x['pool']['id'], x['user']['id'], x['fromState'], x['toState'], x['timestamp']])
usage = pd.DataFrame(list_, columns = ['usage-as-collateral-id', 'pool-id', 'user-id', 'from-state', 'to-state', 'timestamp'])
usage['date'] = usage['timestamp'].apply(lambda x: datetime.fromtimestamp(x))
usage.drop('timestamp', axis=1, inplace=True)

# import flash_loans data
with open('data/flash_loans.json') as json_file:
    flash_loan = json.load(json_file)
data_list = []
for x in flash_loan:
    data_list += list(list(x.values())[0].values())[0]
list_ = []
for x in data_list:
    list_.append([x['id'], x['pool']['id'], x['target'], x['amount'], x['totalFee'], x['protocolFee'], x['timestamp']])
flash = pd.DataFrame(list_, columns = ['flash-loan-id', 'pool-id', 'target', 'amount', 'total-fee', 'protocol-fee', 'timestamp'])
flash['date'] = flash['timestamp'].apply(lambda x: datetime.fromtimestamp(x))
flash.drop('timestamp', axis=1, inplace=True)

borr.sort_values('date', inplace=True, ascending=False)
dep.sort_values('date', inplace=True, ascending=False)
liq.sort_values('date', inplace=True, ascending=False)
orig.sort_values('date', inplace=True, ascending=False)
reb.sort_values('date', inplace=True, ascending=False)
red.sort_values('date', inplace=True, ascending=False)
rep.sort_values('date', inplace=True, ascending=False)
swap.sort_values('date', inplace=True, ascending=False)
usage.sort_values('date', inplace=True, ascending=False)
flash.sort_values('date', inplace=True, ascending=False)

# prints the date range that each method exist in
"""print(borr['date'][0], ' - ', borr['date'].iloc[-1])
print(dep['date'][0], ' - ', dep['date'].iloc[-1])
print(liq['date'][0], ' - ', liq['date'].iloc[-1])
print(orig['date'][0], ' - ', orig['date'].iloc[-1])
print(red['date'][0], ' - ', red['date'].iloc[-1])
print(reb['date'][0], ' - ', reb['date'].iloc[-1])
print(rep['date'][0], ' - ', rep['date'].iloc[-1])
print(swap['date'][0], ' - ', swap['date'].iloc[-1])
print(usage['date'][0], ' - ', usage['date'].iloc[-1])
print(flash['date'][0], ' - ', flash['date'].iloc[-1])
"""

print(borr.head())
print(dep.head())
print(liq.head())
print(orig.head())
print(red.head())
print(reb.head())
print(red.head())
print(swap.head())
print(usage.head())
print(flash.head())
