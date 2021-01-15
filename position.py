# coding=utf-8
# 
# Extract cash and holding positions from BOCI Trustee valuation 
# report to Geneva reconciliation files.
# 

from toolz.functoolz import compose
from steven_utils.utility import writeCsv
from stbf.valuation_report import getValuationDataFromFile
from functools import partial
from itertools import chain
import logging
logger = logging.getLogger(__name__)



def processValuationFile(file):
	"""
	[String] file 
		=> [Tuple] ( [String] cash recon file
				   , [String] position recon file
				   )
	"""
	logger.debug('processValuationFile(): {0}'.format(file))

	date, _, _, bondPositions, cashPositions = \
		getValuationDataFromFile(file)

	return ( createCashReconFile(getParentFolder(file), date, cashPositions)
		   , createPositionReconFile(getParentFolder(file), date, bondPositions))



getPortfolioName = lambda : 'Short Term Bond Fund'


getCashReconFields = lambda : \
	('portfolio', 'custodian', 'date', 'currency', 'balance')


getPositionReconFields = lambda : \
	( 'portfolio', 'custodian', 'date', 'geneva_investment_id'
	, 'ISIN', 'bloomberg_figi', 'name', 'currency', 'quantity'
	)


dictToValues = lambda keys, d: \
	map(lambda key: d[key], keys)



"""
	[String] date,
	[Dictionary] cash position
		=> [Dictionary] cash reconciliation position
"""
cashReconPosition = lambda date, position: \
	{ 'portfolio': getPortfolioName()
	, 'custodian': ''
	, 'date': date
	, 'currency': position['DEAL CCY']
	, 'balance': position['ORIG CURR BOOK COST']
	}



"""
	[String] date,
	[Dictionary] fixed deposit position
		=> [Dictionary] holding reconciliation position
"""
# fixedDepositReconPosition = lambda date, fd: \
# 	{ 'portfolio': getPortfolioName()
# 	, 'custodian': ''
# 	, 'date': date
# 	, 'geneva_investment_id': getFixedDepositInvestId(fd)
# 	, 'ISIN': ''
# 	, 'bloomberg_figi': ''
# 	, 'name': fd['INVESTMENT']
# 	, 'currency': fd['DEAL CCY']
# 	, 'quantity': fd['ORIG CURR BOOK COST']
# 	}



"""
	[String] date,
	[Dictionary] bond position
		=> [Dictionary] holding reconciliation position
"""
bondReconPosition = lambda date, bond: \
	{ 'portfolio': getPortfolioName()
	, 'custodian': ''
	, 'date': date
	, 'geneva_investment_id': ''
	, 'ISIN': bond['ISIN CODE']
	, 'bloomberg_figi': ''
	, 'name': bond['INVESTMENT']
	, 'currency': bond['DEAL CCY']
	, 'quantity': bond['NOMINAL QUANTITY']
	}


# [String] file name => [String] suffix 
getParentFolder = compose(
	lambda L: '_'.join(L)
  , lambda s: s.split()
  , lambda file: file.split('\\')[-2]
)
	


def createCashReconFile(prefix, date, cashPositions):
	"""
	[String] prefix, [String] date, [Iterable] cash positions
		=> [String] cash reconciliation file name

	Side effect: write a csv file
	"""
	return \
	compose(
		lambda values: \
			writeCsv( prefix + '_cash_' + date + '.csv'
					, chain([getCashReconFields()], values)
					, delimiter='|'
					)
	  , partial(map, partial(dictToValues, getCashReconFields()))
	  , partial(map, partial(cashReconPosition, date))
	)(cashPositions)



def createPositionReconFile(prefix, date, bondPositions):
	"""
	[String] prefix, [String] date, [Iterable] bond positions
		=> [String] holding reconciliation file name

	Side effect: write a csv file
	"""
	return \
	compose(
		lambda values: \
			writeCsv( prefix + '_position_' + date + '.csv'
					, chain([getPositionReconFields()], values)
					, delimiter='|'
					)
	  , partial(map, partial(dictToValues, getPositionReconFields()))
	  , partial(map, partial(bondReconPosition, date))
	)(bondPositions)