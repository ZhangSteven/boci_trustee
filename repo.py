# coding=utf-8
# 
# Converts Bloomberg THRP repo trade file to BOCI-Prudential repo trade file 
# format.
# 
from boci_trustee.trade import toDateTimeString, toStringIfFloat \
							, skipFirst2Lines, getAccountNumber
from clamc_datafeed.feeder import mergeDictionary
from utils.iter import firstOf
from utils.excel import getRawPositions
from toolz.itertoolz import groupby as groupbyToolz
from toolz.functoolz import compose
from functools import partial
from itertools import dropwhile
import logging
logger = logging.getLogger(__name__)



def getRepoTrades(lines):
	"""
	[Iterator] tickets (Bloomberg REPO tickets) 
		=> [Iterator] trades (BOCI trustee REPO trades)
	"""
	groupIdentity = lambda tkt: \
		tkt['Fund'] + toStringIfFloat(tkt['Trd Dt']) + toStringIfFloat(tkt['Stl Date']) + \
		tkt['Crcy'] + tkt['Broker ID'] + toStringIfFloat(100.0 * tkt['Repo Rte']) + \
		toStringIfFloat(tkt['Unadj Term Money']) + tkt['Repo Sta']


	"""
		[Iterator] lines (from Bloomberg THRP Repo trade file) 
			=> [Iterator] repo tickets
	"""
	getRepoTickets = compose(
		getRawPositions
	  , partial(dropwhile, lambda L: len(L) == 0 or L[0] == '')
	  , skipFirst2Lines
	)


	return map( ticketToTrade
			  , map( groupToTicket
			  	   , groupbyToolz( groupIdentity
			  	   				 , getRepoTickets(lines)
			  	   				 ).values()))



def groupToTicket(group):
	"""
	[List] group of tickets => [Dictionary] ticket
	"""
	isActiveTicket = lambda g: \
		g[0]['Repo Sta'] == 'Active'


	getMrcTicket = lambda g: \
		firstOf(lambda el: el['Type'] == 'MRC', g)

	getRtTicket = lambda g: \
		firstOf(lambda el: el['Type'] == 'RT', g)

	getKmrTicket = lambda g: \
		firstOf(lambda el: el['Type'] == 'KMR', g)

	getCrTicket = lambda g: \
		firstOf(lambda el: el['Type'] == 'CR', g)


	return \
	mergeDictionary(getRtTicket(group), {'Tkt #': getMrcTicket(group)['Tkt #']}) \
	if isActiveTicket(group) else \
	mergeDictionary(getCrTicket(group), {'Orig Tkt': getKmrTicket(group)['Orig Tkt']})



def toNumber(x):
	"""
	[String or Float] x => [Float] result
	"""
	try:
		return float(x)
	except ValueError:
		if isinstance(x, str) and len(x) > 1 and x[-1] == 'M':
			return 1000 * toNumber(x[0:-1])

	logger.error('toNumber(): invalid input: {0}, {1}'.format(x, type(x)))
	raise ValueError



def ticketToTrade(ticket):
	"""
	[Dictionary] REPO trade ticket (Bloomberg)
		=> [Dictionary] BOCI Prudential trade
	"""
	t = {}
	t['Portfolio_code'] = getAccountNumber(ticket['Fund'])
	t['Txn_type'] = 'REPO'
	t['Txn_sub_type'] = 'Close' if ticket['Repo Sta'] == 'Closed' else \
						'Change Rate' if ticket['Trd Dt'] > ticket['Stl Date'] else \
						'Open'
	t['Trade_date'] = toDateTimeString(ticket['Trd Dt']) if t['Txn_sub_type'] == 'Open' \
						else ''
	t['Settle_date'] = toDateTimeString(ticket['Stl Date']) if t['Txn_sub_type'] == 'Open' \
						else ''
	t['Mature_date'] = '' if t['Txn_sub_type'] == 'Change Rate' else \
						ticket['Trd Dt'] if t['Txn_sub_type'] == 'Close' else \
						'31/12/2049' if ticket['Trm Date'] == 'OPEN' else \
						toDateTimeString(ticket['Trm Date'])
	t['Loan_ccy'] = '' if t['Txn_sub_type'] == 'Change Rate' else ticket['Crcy']
	t['Amount'] = '' if t['Txn_sub_type'] == 'Change Rate' else ticket['Loan Amount']
	t['Eff_date'] = '' if t['Txn_sub_type'] == 'Close' else \
					toDateTimeString(ticket['Stl Date']) if t['Txn_sub_type'] == 'Open' \
					else toDateTimeString(ticket['Trd Dt'])
	t['Int_rate'] = '' if t['Txn_sub_type'] == 'Close' else ticket['Repo Rte']
	t['Int_mode'] = '' if t['Txn_sub_type'] == 'Close' else 'ACT/360'
	t['Col_ISIN'] = '' if t['Txn_sub_type'] == 'Change Rate' else ticket['ISIN']
	t['Col_Qty'] = '' if t['Txn_sub_type'] == 'Change Rate' else \
					1000 * toNumber(ticket['Amount'])
	t['Broker'] = '' if t['Txn_sub_type'] == 'Change Rate' else ticket['Broker ID']
	t['Cust_ref'] = toStringIfFloat(ticket['Orig Tkt']) if t['Txn_sub_type'] == 'Close' \
					else toStringIfFloat(ticket['Tkt #'])


	return mergeDictionary( t
						  , { 'Col_SEDOL': ''
						    , 'Col_Bloomberg': ''
						    , 'Col_LocalCode': ''
						    , 'Col_CMUCode': ''
						    , 'Col_desc': ''
						    , 'Exchange': ''
						    }
						  )



getRepoCsvHeaders = lambda: \
	[ 'Portfolio_code',	'Txn_type',	'Txn_sub_type',	'Trade_date', 'Settle_date'
	, 'Mature_date', 'Loan_ccy', 'Amount', 'Eff_date', 'Int_rate', 'Int_mode'
	, 'Col_ISIN', 'Col_SEDOL', 'Col_Bloomberg',	'Col_LocalCode', 'Col_CMUCode'
	, 'Col_desc', 'Col_Qty', 'Broker', 'Exchange', 'Cust_ref'
	]