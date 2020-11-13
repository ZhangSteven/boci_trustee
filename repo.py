# coding=utf-8
# 
# Converts Bloomberg THRP repo trade file to BOCI-Prudential repo trade file 
# format.
# 
from boci_trustee.trade import toDateTimeString, toStringIfFloat
from clamc_datafeed.feeder import mergeDictionary
from utils.iter import firstOf
from toolz.itertoolz import groupby as groupbyToolz
import logging
logger = logging.getLogger(__name__)



def processRepo(lines):
	"""
	[Iterator] lines (from Bloomberg THRP Repo trade file)
		=> ( [String] output csv file
		   , [String] message subject
		   , [String] message body
		   )
	"""
	pass



def getRepoTickets(lines):
	"""
	[Iterator] lines (from Bloomberg THRP Repo trade file) 
		=> [Iterator] repo tickets
	"""
	pass



def convert(tickets):
	"""
	[Iterator] tickets (Bloomberg REPO tickets) 
		=> [Iterator] trades (BOCI trustee REPO trades)
	"""
	groupIdentity = lambda tkt: \
		tkt['Fund'] + toStringIfFloat(tkt['Trd Dt']) + toStringIfFloat(tkt['Stl Date']) + \
		tkt['Crcy'] + tkt['Broker ID'] + toStringIfFloat(100.0 * tkt['Repo Rte']) + \
		toStringIfFloat(tkt['Unadj Term Money']) + tkt['Repo Sta']


	return map(ticketToTrade, map(groupToTicket, groupbyToolz(groupIdentity, tickets)))



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
	if isinstance(x, float):
		return x

	if isinstance(x, str) and len(x) > 1 and x[-1] == 'M':
		return 1000 * toNumber(x[0:-1])

	logger.error('toNumber(): invalid input: {0}'.format(x))
	raise ValueError



def ticketToTrade(ticket):
	"""
	[Dictionary] REPO trade ticket (Bloomberg)
		=> [Dictionary] BOCI Prudential trade
	"""
	t = {}
	t['Txn_type'] = 'REPO'
	t['Txn_sub_type'] = 'Close' if ticket['Repo Sta'] == 'Closed' else \
						'Change Rate' if ticket['Trd Dt'] > ticket['Stl Dt'] else \
						'Open'
	t['Trade_date'] = toDateTimeString(ticket['Trd Dt']) if t['Txn_sub_type'] == 'Open' \
						else ''
	t['Settle_date'] = toDateTimeString(ticket['Stl Date']) if t['Txn_sub_type'] == 'Open' \
						else ''
	t['Mature_date'] = '' if t['Txn_sub_type'] == 'Change Rate' else \
						'OPEN' if ticket['Trm Date'] == 'OPEN' else \
						toDateTimeString(ticket['Trm Date'])
	t['Loan_ccy'] = '' if t['Txn_sub_type'] == 'Change Rate' else ticket['Crcy']
	t['Amount'] = '' if t['Txn_sub_type'] == 'Change Rate' else ticket['Loan Amount']
	t['Eff_date'] = '' if t['Txn_sub_type'] == 'Close' else \
					toDateTimeString(ticket['Stl Date']) if t['Txn_sub_type'] == 'Open' \
					toDateTimeString(ticket['Trd Dt'])
	t['Int_rate'] = '' if t['Txn_sub_type'] == 'Close' else t['Repo Rte']
	t['Int_mode'] = '' if t['Txn_sub_type'] == 'Close' else 'ACT/360'
	t['Col_ISIN'] = '' if t['Txn_sub_type'] == 'Change Rate' else ticket['ISIN']
	t['Col_Qty'] = '' if t['Txn_sub_type'] == 'Change Rate' else \
					1000 * toNumber(ticket['Amount'])
	t['Broker'] = '' if t['Txn_sub_type'] == 'Change Rate' else ticket['Broker ID']
	t['Cust_ref'] = toStringIfFloat(ticket['Orig Tkt']) if t['Txn_sub_type'] == 'Close' \
					else toStringIfFloat(t['Tkt #'])


	return mergeDictionary( t
						  , { 'Col_SEDOL': ''
						    , 'Col_Bloomberg': ''
						    , 'Col_LocalCode': ''
						    , 'Col_CMUCode': ''
						    , 'Col_desc': ''
						    , 'Exchange': ''
						    }
						  )