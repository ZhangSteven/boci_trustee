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


# Compare repo_new.py and repo.py, there is a difference in bociClose() function
# the logic is different.



"""
Questions:

1) borrow money from others, type = REPO, what about lend money to others?

2) what about cancel trade



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



getRepoType = lambda transactionType: \
	'REPO' if transactionType == 'ReverseRepo_InsertUpdate' else 'REPO'



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



getISIN = lambda sidType, sid: \
	sid if sidType == 'ISIN' else lognRaise('{0}={1}'.format(sidType, sid))



def bociTrade(repoData, tradeInfo):
	"""
	[Dictionary] repo data from datastore ([String] user tran id -> [Dictionary])
	[Dictionary] repo trade data from xml
		=> [Dictionary] boci repo trade
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
	, 'Int_mode': repoData[tradeInfo['UserTranId1']]['DayCount']
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



def bociClose(repoData, closeInfo):
	"""
	[Dictionary] repo data from datastore ([String] user tran id -> [Dictionary])
	[Dictionary] repo close trade data from xml
		=> [Dictionary] boci repo close data
	"""
	logger.debug('bociClose(): {0}'.format(tradeInfo['UserTranId1']))

	return \
	{ 'Portfolio_code': getAccountNumber(repoData[tradeInfo['userTranId1']]['Portfolio'])
	, 'Txn_type': getRepoType(tradeInfo['TransactionType'])
	, 'Txn_sub_type': 'Close'
	, 'Trade_date': ''
	, 'Settle_date': ''
	, 'Mature_date': changeDateFormat(tradeInfo['ActualSettleDate'])
	, 'Loan_ccy': repoData[tradeInfo['userTranId1']]['Currency']
	, 'Amount': repoData[tradeInfo['userTranId1']]['CollateralValue']
	, 'Eff_date': ''
	, 'Int_rate': ''
	, 'Int_mode': ''
	, 'Col_ISIN': getISIN( repoData[tradeInfo['userTranId1']]['CollateralIDType']
						 , repoData[tradeInfo['userTranId1']]['CollateralID']
						 )
	, 'Col_SEDOL': ''
	, 'Col_Bloomberg': ''
	, 'Col_LocalCode': ''
	, 'Col_CMUCode': ''
	, 'Col_desc': ''
	, 'Col_Qty': repoData[tradeInfo['userTranId1']]['Quantity']
	, 'Broker': repoData[tradeInfo['userTranId1']]['Broker']
	, 'Exchange': ''
	, 'Cust_ref': tradeInfo['UserTranId1']
	}



def bociCancel(repoData, cancelInfo):
	"""
	[Dictionary] repo data from datastore ([String] user tran id -> [Dictionary])
	[Dictionary] repo cancel trade data 
		=> [Dictionary] boci repo cancel data
	"""
	return {}



def bociRerate(repoData, rerateInfo):
	"""
	[Dictionary] repo data from datastore ([String] user tran id -> [Dictionary])
	[Dictionary] repo rerate data => [Dictionary] boci repo rerate data
	"""
	logger.debug('bociRerate(): {0}'.format(tradeInfo['UserTranId1']))

	return \
	{ 'Portfolio_code': getAccountNumber(repoData[tradeInfo['userTranId1']]['Portfolio'])
	, 'Txn_type': getRepoType(tradeInfo['TransactionType'])
	, 'Txn_sub_type': 'Change Rate'
	, 'Trade_date': ''
	, 'Settle_date': ''
	, 'Mature_date': ''
	, 'Loan_ccy': ''
	, 'Amount': ''
	, 'Eff_date': changeDateFormat(rerateInfo['RateDate'])
	, 'Int_rate': rerateInfo['Rate']
	, 'Int_mode': repoData[tradeInfo['UserTranId1']]['DayCount']
	, 'Col_ISIN': ''
	, 'Col_SEDOL': ''
	, 'Col_Bloomberg': ''
	, 'Col_LocalCode': ''
	, 'Col_CMUCode': ''
	, 'Col_desc': ''
	, 'Col_Qty': ''
	, 'Broker': ''
	, 'Exchange': ''
	, 'Cust_ref': tradeInfo['UserTranId1']
	}




