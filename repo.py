# coding=utf-8
# 
# Read Bloomberg repo XML trade file and repo XML rerate file, then convert them
# to BOCI-Prudential repo trade file format.
# 
# It assumes repo master information is already in the repo database.
# 
from aim_xml.add_header import isRepoTrade, isRepoRerate
from aim_xml.repo_xml import getRepoTradeFromFile, getRepoRerateFromFile
from repo_data.data import initializeDatastore, getRepo
from repo_data.repo_datastore import isRepoOpenTrade, isRepoCloseTrade, isRepoCancelTrade
from steven_utils.file import getFiles
from boci_trustee.utility import getCurrentDir
from toolz.functoolz import compose
from itertools import chain
from functools import partial, reduce
import logging
logger = logging.getLogger(__name__)



"""
Questions:

1) borrow money from others, type = REPO, what about lend money to others?

2) cancel trade how to specify



1 get repo trade file and repo rerate file;

2 convert trade file to repo trade data (iterable);

3 convert rerate file to repo rerate data (iterable);

4 combine them and write to output csv

5 notify user about the result, with output csv as attachment 
(no repo, success, error)
"""
def checkAtMostOne(L):
	if len(L) > 1:
		logger.error('too many files')
		raise ValueError
	else:
		return L



def getRepoTradeFiles(directory):
	"""
	[String] => [List] ([String] trade file)

	If there is no repo trade file in the directory, return an empty list.
	If there is one, return a list of one file.
	If there are more than one, raise exception.
	"""
	logger.debug('getRepoTradeFiles(): {0}'.format(directory))

	return \
	compose(
		checkAtMostOne
	  , list
	  , partial(filter, isRepoTrade)
	  , lambda directory: getFiles(directory, True)
	)(directory)



def getRepoRerateFiles(directory):
	"""
	[String] => [List] ([String] rerate file)

	If there is no repo rerate file in the directory, return an empty list.
	If there is one, return a list of one file.
	If there are more than one, raise exception.
	"""
	logger.debug('getRepoRerateFiles(): {0}'.format(directory))
	
	return \
	compose(
		checkAtMostOne
	  , list
	  , partial(filter, isRepoRerate)
	  , lambda directory: getFiles(directory, True)
	)(directory)



def readRepoTradeFile(file):
	"""
	[String] file => 
		( [Iterable] ([Dictionary] repo trade data)
		, [Iterable] ([Dictionary] repo close trade data)
		, [Iterable] ([Dictionary] repo cancel trade data)
		)
	"""
	logger.debug('readRepoTradeFile(): {0}'.format(file))

	def lognRaise(msg):
		logger.error(msg)
		raise ValueError


	def addTrade(acc, el):
		return \
		(chain(acc[0], [el]), acc[1], acc[2]) if isRepoOpenTrade(el) else \
		(acc[0], chain(acc[1], [el]), acc[2]) if isRepoCloseTrade(el) else \
		(acc[0], acc[1], chain(acc[2], [el])) if isRepoCancelTrade(el) else \
		lognRaise('invalid trade type: {0}'.format(el))


	return reduce(addTrade, getRepoTradeFromFile, ([], [], []))



def readRepoRerateFile(file):
	"""
	[String] file => [Iterable] ([Dictionary] repo rerate data)
	"""
	def rerateEntry(el):
		"""
		[Dictionary] el => [Dictionary] rerate entry
		"""
		if el['Loan'].startswith('UserTranId1='):
			return { 'UserTranId1': el['Loan'][12:]
				   , 'RateDate': el['RateTable']['RateDate']
				   , 'Rate': el['RateTable']['Rate']
				   }
		else:
			logger.error('rerateEntry(): failed to find UserTranId1: {0}'.format(el['Loan']))
			raise ValueError


	return map(rerateEntry, getRepoRerateFromFile(file))



getBociRepoHeaders = lambda: \
	[ 'Portfolio_code',	'Txn_type',	'Txn_sub_type',	'Trade_date', 'Settle_date'
	, 'Mature_date', 'Loan_ccy', 'Amount', 'Eff_date', 'Int_rate', 'Int_mode'
	, 'Col_ISIN', 'Col_SEDOL', 'Col_Bloomberg',	'Col_LocalCode', 'Col_CMUCode'
	, 'Col_desc', 'Col_Qty', 'Broker', 'Exchange', 'Cust_ref'
	]



def lognRaise(msg):
	logger.error(msg)
	raise ValueError



# [String] dt (yyyy-mm-ddTHH:MM:SS) => [String] dd/mm/yyyy
changeDateFormat = compose(
	lambda L: L[2] + '/' + L[1] + '/' + L[0]
  , lambda s: s.split('-')
  , lambda s: s.split('T')[0]
)



"""
	[String] accountCode => [String] BOCK trustee account number

	Map Bloomberg account code to BOCI Trustee's account number
"""
getAccountNumber = lambda accountCode: \
	{ '60001': 'CLAMC STBD'
	, 'TEST_R': 'Test Repo'	# for testing only
	}[accountCode]



"""
	[String] transaction type => [String] BOCI repo type
	
	Bloomberg AIM's reverse repo is borrowing money from counterparty, 
	mapping to BOCI's REPO type, while Bloomberg's repo is lending money
	to counterparty, mapping to BOCK's REVERSE REPO
"""
getRepoType = lambda transactionType: \
	'REPO' if transactionType == 'ReverseRepo_InsertUpdate' else \
	'REVERSE REPO'



"""
	[String] investment string => [String] ISIN

The investment string is something like: Isin=XS12345678. Because most
repo collaterals are bond, and Bloomberg gives their ISIN code as the
collateral id. So here we implement the ISIN code version first. In the
future, when we have other types of collateral, Bloomberg may give their
ticker as collateral id, then we need to improve this function.
"""
getCollateralISIN = compose(
	lambda L: L[1] if L[0].lower() == 'isin' else lognRaise(
				'getCollateralISIN(): {0}'.format(L))
  , lambda s: s.split('=')
)



def bociTrade(repoMaster, tradeInfo):
	"""
	[Dictionary] repo master data
	[Dictionary] repo trade data => [Dictionary] boci repo trade data
	"""
	logger.debug('bociTrade(): {0}'.format(tradeInfo['UserTranId1']))

	return \
	{ 'Portfolio_code': getAccountNumber(tradeInfo['Portfolio'])
	, 'Txn_type': getRepoType(tradeInfo['TransactionType'])
	, 'Txn_sub_type': 'Open'
	, 'Trade_date': changeDateFormat(tradeInfo['EventDate'])
	, 'Settle_date': changeDateFormat(tradeInfo['SettleDate'])
	, 'Mature_date': '31/12/2049' if tradeInfo['ActualSettleDate'] == 'CALC' else \
					changeDateFormat(tradeInfo['ActualSettleDate'])
	, 'Loan_ccy': tradeInfo['CounterInvestment']
	, 'Amount': tradeInfo['NetCounterAmount']
	, 'Eff_date': changeDateFormat(tradeInfo['SettleDate'])
	, 'Int_rate': tradeInfo['Coupon']
	, 'Int_mode': repoMaster[tradeInfo['RepoName']]['DayCount']
	, 'Col_ISIN': getCollateralISIN(tradeInfo['Investment'])
	, 'Col_SEDOL': ''
	, 'Col_Bloomberg': ''
	, 'Col_LocalCode': ''
	, 'Col_CMUCode': ''
	, 'Col_desc': ''
	, 'Col_Qty': tradeInfo['Quantity']
	, 'Broker': tradeInfo['Broker']
	, 'Exchange': ''
	, 'Cust_ref': tradeInfo['UserTranId1']
	}



def bociClose(oldRepoTrade, currentRepoTrade, closeInfo):
	"""
	[Dictionary] old repo trade info (from data base)
	[Dictionary] current repo trade (from trade file)
	[Dictionary] repo close trade data 
		=> [Dictionary] boci repo close data
	"""
	logger.debug('bociClose(): {0}'.format(tradeInfo['UserTranId1']))

	getCurrency = lambda oldRepoTrade, currentRepoTrade, userTranId1: \
		oldRepoTrade[userTranId1]['Currency'] if userTranId1 in oldRepoTrade else \
		currentRepoTrade[userTranId1]['CounterInvestment'] if userTranId1 in currentRepoTrade \
		else lognRaise('failed to get currency')


	getAmount = lambda oldRepoTrade, currentRepoTrade, userTranId1: \
		oldRepoTrade[userTranId1]['LoanAmount'] if userTranId1 in oldRepoTrade else \
		currentRepoTrade[userTranId1]['NetCounterAmount'] if userTranId1 in currentRepoTrade \
		else lognRaise('failed to get amount')


	getISIN = lambda oldRepoTrade, currentRepoTrade, userTranId1: \
		getCollateralISIN(currentRepoTrade[userTranId1]['Investment']) \
		if userTranId1 in currentRepoTrade else \
		oldRepoTrade[userTranId1]['CollateralID'] if userTranId1 in oldRepoTrade \
		and oldRepoTrade[userTranId1]['CollateralIDType'] == 'ISIN' else \
		lognRaise('failed to get ISIN')


	getQuantity = lambda oldRepoTrade, currentRepoTrade, userTranId1: \
		oldRepoTrade[userTranId1]['Quantity'] if userTranId1 in oldRepoTrade else \
		currentRepoTrade[userTranId1]['Quantity'] if userTranId1 in currentRepoTrade \
		else lognRaise('failed to get quantity')


	getBroker = lambda oldRepoTrade, currentRepoTrade, userTranId1: \
		oldRepoTrade[userTranId1]['Broker'] if userTranId1 in oldRepoTrade else \
		currentRepoTrade[userTranId1]['Broker'] if userTranId1 in currentRepoTrade \
		else lognRaise('failed to get broker')


	return \
	{ 'Portfolio_code': getAccountNumber(tradeInfo['Portfolio'])
	, 'Txn_type': getRepoType(tradeInfo['TransactionType'])
	, 'Txn_sub_type': 'Close'
	, 'Trade_date': ''
	, 'Settle_date': ''
	, 'Mature_date': changeDateFormat(tradeInfo['ActualSettleDate'])
	, 'Loan_ccy': getCurrency(oldRepoTrade, currentRepoTrade, tradeInfo['userTranId1'])
	, 'Amount': getAmount(oldRepoTrade, currentRepoTrade, tradeInfo['userTranId1'])
	, 'Eff_date': ''
	, 'Int_rate': ''
	, 'Int_mode': ''
	, 'Col_ISIN': getISIN(oldRepoTrade, currentRepoTrade, tradeInfo['userTranId1'])
	, 'Col_SEDOL': ''
	, 'Col_Bloomberg': ''
	, 'Col_LocalCode': ''
	, 'Col_CMUCode': ''
	, 'Col_desc': ''
	, 'Col_Qty': getQuantity(oldRepoTrade, currentRepoTrade, tradeInfo['userTranId1'])
	, 'Broker': getQuantity(oldRepoTrade, currentRepoTrade, tradeInfo['userTranId1'])
	, 'Exchange': ''
	, 'Cust_ref': tradeInfo['UserTranId1']
	}


def bociCancel(cancelInfo):
	"""
	[Dictionary] repo cancel trade data => [Dictionary] boci repo cancel data
	
	We need to switch to manuall processing if there are any cancel 
	trades.
	"""
	return {}



def bociRerate(rerateInfo):
	"""
	[Dictionary] repo rerate data => [Dictionary] boci repo rerate data
	"""
	return {}






