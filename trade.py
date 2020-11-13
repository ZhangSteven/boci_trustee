# coding=utf-8
# 
# Converts Bloomberg trade file to BOCI-Prudential trade file format and
# send notification email
# 
from boci_trustee.utility import getInputDirectory, getOutputDirectory\
						, getMailSender, getMailRecipients, getMailServer\
						, getMailTimeout, getBrokerSSIFile, getCurrentDir
from toolz.functoolz import compose
from functools import partial, lru_cache
from itertools import dropwhile
from utils.file import getFiles
from utils.mail import sendMail
from utils.utility import writeCsv, fromExcelOrdinal
from utils.excel import fileToLines, getRawPositions
from utils.iter import pop
from os.path import join
import shutil
import logging
logger = logging.getLogger(__name__)



def sendNotification(tradeWithMultipleSSI, outputFile):
	"""
	input: [String] subject (email subject), [String] body (email body)
	
	side effect: send notification email to recipients with the subject and body.
	"""
	subject = 'Successfully converted BOCI-Prudential trade file' \
				if tradeWithMultipleSSI == [] else \
				'Warning: BOCI-Prudential trade file: Broker with Muitple SSI'

	body = outputFile if tradeWithMultipleSSI == [] else \
				'\n'.join([outputFile] + tradeWithMultipleSSI)

	# sendMail( body, subject, getMailSender(), getMailRecipients()\
	# 		, getMailServer(), getMailTimeout())
	print('send mail:', subject, body) # for debugging only



def getInputFile():
	logger.debug('getInputFile(): searching for files in {0}'.format(getInputDirectory()))

	def checkOnlyOne(L):
		if len(L) != 1:
			raise ValueError('should be only one file, but {0} found'.format(len(L)))

		return L


	return \
	compose(
		lambda L: L[0]
	  , checkOnlyOne
	  , list
	  , partial(filter, lambda f: f.startswith('TD') and f.endswith('.xlsx'))
	  , getFiles
	)(getInputDirectory())



def getBlpTradesFromFile(inputFile):
	"""
	[String] inputFile => [Iterable] trades from Bloomberg
	"""
	def skipFirst2Lines(lines):
		pop(lines)
		pop(lines)
		return lines


	return \
	compose(
		getRawPositions
	  , partial(dropwhile, lambda L: len(L) == 0 or L[0] == '')
	  , skipFirst2Lines
	  , fileToLines
	)(inputFile)



"""
	[String] fundName => [String] account number

	Map Bloomberg fund name to BOCI Trustee's account number

	FIXME: find the BOCI trustee's account number
"""
getAccountNumber = lambda fundName: \
	{ '60001': 'CLAMC STBD'
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
	def skipOneLine(lines):
		pop(lines)
		return lines


	return \
	compose(
		list
	  , partial(map, lambda line: (line[0].strip(), toStringIfFloat(line[-1])))
	  , skipOneLine
	  , fileToLines
  	  , partial(join, getCurrentDir(), 'reference')
	)(file)



convert = lambda inputFile: \
	map(bociTrade, getBlpTradesFromFile(inputFile))



def getTradeWithMultipleSSI(trades):
	"""
	[Iterator] trades (boci format) => [List] reference number of trades whose
		broker has multiple SSI codes.
	"""
	withMultipleSSI = lambda t: \
		t['BrokerShortName'] in getBrokerWithMultipleSSI(getBrokerSSIFile())


	return list(map( lambda t: t['TradeReferenceNumber']
				   , filter(withMultipleSSI, trades)))



def output(trades, outputDirectory):
	"""
	[Iterator] trades
	[String] outputDirectory
		=> [String] outputFile

	Write BOCI trades to output file, then return its file name.
	"""
	outputHeaders = \
	[ 'Account', 'SEDOL', 'ISIN', 'Name', 'TranType', 'Quantity', 'TradeDate'
	, 'SettlementDate', 'Currency', 'Price', 'AccurredInterest', 'SettlementAmount'
	, 'Commission', 'StampDuty', 'TransactionLevy', 'ClearingFee', 'SalesTax'
	, 'HongKongCCASSFee', 'TradeReferenceNumber', 'BrokerCode', 'BrokerName']


	dictToValues = lambda d: \
		map(lambda h: d.get(h, ''), outputHeaders)


	return writeCsv( join(getOutputDirectory(), 'output.csv')
				   , map(dictToValues, trades))





if __name__ == '__main__':
	import logging.config
	logging.config.fileConfig('logging.config', disable_existing_loggers=False)

	"""
	Do the following:

	1. Convert the input file to output csv file.
	2. Send notification email.
	3. Move the input file to 'processed' sub directory.
	"""
	logger.debug('main: start')
	try:
		inputFile = getInputFile()
		trades = list(convert(join(getInputDirectory(), inputFile)))
		sendNotification( getTradeWithMultipleSSI(trades)
						, output(trades, getOutputDirectory()))

		# shutil.move( join(getInputDirectory(), inputFile)
		# 		   , join(getInputDirectory(), 'processed', inputFile))

	except:
		logger.exception('main')
		sendNotification('Error occurred when converting BOCI-Prudential trade file', '')