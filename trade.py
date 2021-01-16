# coding=utf-8
# 
# Converts Bloomberg trade file to BOCI-Prudential trade file format and
# send notification email
# 
from boci_trustee.utility import getBrokerSSIFile, getCurrentDir
from toolz.functoolz import compose
from functools import partial, lru_cache
from itertools import dropwhile
from steven_utils.excel import fromExcelOrdinal, fileToLines \
							, getRawPositionsFromLines
from steven_utils.iter import skipN
from os.path import join
import shutil
import logging
logger = logging.getLogger(__name__)



"""
	[String] fundName => [String] account number

	Map Bloomberg fund name to BOCI Trustee's account number

	FIXME: find the BOCI trustee's account number
"""
getAccountNumber = lambda fundName: \
	{ '60001': 'CLAMC STBD'
	, '40017-B': '666666'	# for testing only
	, '40019': '12345678'	# for testing only
	, '19437-A': '19437A'	# for testing only
	, '19437-B': '19437B'	# for testing only
	}[fundName]



def getBrokerCode(brokerName):
	"""
	[String] brokerName => [String] broker SSI code

	Map the broker code to broker SSI code
	"""
	@lru_cache(maxsize=3)
	def getBrokerSSIMapping(file):
		return dict(loadBrokerSSIMappingFromFile(file))


	return getBrokerSSIMapping(getBrokerSSIFile())[brokerName]



def duplidateItems(L):
	"""
	[Iterator] L => [Set] duplicate items

	Go through the items in L, find whether there are duplicates and return
	all items that are duplicated at least once in a set.
	"""
	uniqueItems = set()
	duplicates = set()
	for x in L:
		if x in uniqueItems:
			duplicates.add(x)
		else:
			uniqueItems.add(x)

	return duplicates



@lru_cache(maxsize=3)
def getBrokerWithMultipleSSI(file):
	return \
	compose(
		duplidateItems
	  , partial(map, lambda el: el[0])
	  , loadBrokerSSIMappingFromFile
	)(file)



toStringIfFloat = lambda x: \
	str(int(x)) if isinstance(x, float) else x



toDateTimeString = lambda x: \
	fromExcelOrdinal(x).strftime('%d/%m/%Y')



def bociTrade(blpTrade):
	"""
	[Dictionary] blpTrade => [Dictionary] bociTrade

	Convert a Bloomberg trade to a different format.

	NOTE: the following mapping only works for bond trade. Because for the moment
	the short term bond fund only trades bond. If equity trade is added, then we
	must change the logic here.
	"""
	return \
	{ 'Account': getAccountNumber(toStringIfFloat(blpTrade['Fund']))
	, 'SEDOL': blpTrade['Sedol']
	, 'ISIN': blpTrade['ISIN']
	, 'Name': blpTrade['Shrt Name']
	, 'TranType': blpTrade['B/S']
	, 'Quantity': blpTrade['Amount Pennies']
	, 'TradeDate': toDateTimeString(blpTrade['As of Dt'])
	, 'SettlementDate': toDateTimeString(blpTrade['Stl Date'])
	, 'Currency': blpTrade['VCurr']
	, 'Price': blpTrade['Price']
	, 'AccurredInterest': blpTrade['Accr Int']
	, 'SettlementAmount': blpTrade['Settle Amount']
	, 'Commission': 0
	, 'StampDuty': ''
	, 'TransactionLevy': ''
	, 'ClearingFee': ''
	, 'SalesTax': ''
	, 'HongKongCCASSFee': ''
	, 'TradeReferenceNumber': toStringIfFloat(blpTrade['Tkt #'])
	, 'BrokerCode': getBrokerCode(blpTrade['FACC Short Name'])
	, 'BrokerName': blpTrade['FACC Long Name']

	, 'BrokerShortName': blpTrade['FACC Short Name'] # to detect multiple SSI
													 # not for output to BOCI
	}



@lru_cache(maxsize=3)
def loadBrokerSSIMappingFromFile(file):
	"""
	[String] broker SSI mapping file 
		=> [List] (sub broker ID, broker SSI)
	"""
	return \
	compose(
		list
	  , partial(map, lambda line: (line[0].strip(), toStringIfFloat(line[-1])))
	  , partial(skipN, 1)
	  , fileToLines
  	  , partial(join, getCurrentDir(), 'reference')
	)(file)



def getTradeWithMultipleSSI(trades):
	"""
	[Iterator] trades (boci format) => [List] reference number of trades whose
		broker has multiple SSI codes.
	"""
	withMultipleSSI = lambda t: \
		t['BrokerShortName'] in getBrokerWithMultipleSSI(getBrokerSSIFile())


	return list(map( lambda t: t['TradeReferenceNumber']
				   , filter(withMultipleSSI, trades)))



"""
	[Iterator] lines => [Iterator] boci trades
"""
convert = lambda mappingFunc, lines: \
compose(
	partial(map, mappingFunc)
  , getRawPositionsFromLines
  , partial(dropwhile, lambda L: len(L) == 0 or L[0] == '')
  , partial(skipN, 2)
)(lines)



"""
	[Iterator] lines => [List] boci trades, [List] trades with multiple SSI
"""
getBociTrades = compose(
	lambda L: (L, getTradeWithMultipleSSI(L))
  , list
  , partial(convert, bociTrade)
)



getTradeCsvHeaders = lambda: \
	[ 'Account', 'SEDOL', 'ISIN', 'Name', 'TranType', 'Quantity', 'TradeDate'
	, 'SettlementDate', 'Currency', 'Price', 'AccurredInterest', 'SettlementAmount'
	, 'Commission', 'StampDuty', 'TransactionLevy', 'ClearingFee', 'SalesTax'
	, 'HongKongCCASSFee', 'TradeReferenceNumber', 'BrokerCode', 'BrokerName'
	]