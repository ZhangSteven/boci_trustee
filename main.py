# coding=utf-8
# 
# Converts Bloomberg trade file to BOCI-Prudential trade file format and
# send notification email
# 
from boci_trustee.utility import getInputDirectory, getOutputDirectory\
						, getMailSender, getMailRecipients, getMailServer\
						, getMailTimeout, getCurrentDir
from boci_trustee.trade import getBociTrades, getTradeCsvHeaders
from toolz.functoolz import compose
from functools import partial
from utils.file import getFiles
from utils.mail import sendMail
from utils.utility import writeCsv
from utils.excel import fileToLines
from utils.iter import pop
from os.path import join
import shutil
import logging
logger = logging.getLogger(__name__)



def processTrade(inputDir, outputDir):
	"""
	[String] inputDir, [String] outputDir
		=> [int] status code, [String] message

	Side effect: 
	(1) move the input file to another directory if processing successful;
	(2) produce an output csv file in the output directory;

	status code: 0:successful, 1:warning, 2:error
	"""
	logger.debug('processTrade(): {0}'.format(inputDir))

	try:
		inputFile = getFileFromDirectory(
						partial(filter, lambda f: f.startswith('TD') and f.endswith('.xlsx'))
					  , inputDir
					)

		trades, tradesWithMultipleSSI = \
			getBociTrades(fileToLines(join(inputDir, inputFile)))

		outputFile = output(trades, getTradeCsvHeaders(), join(outputDir, 'tradeOutput.csv'))

		shutil.move( join(inputDir, inputFile)
				   , join(inputDir, 'processed', inputFile))

		return \
		(0, 'output trade file: ' + outputFile) \
		if tradesWithMultipleSSI == [] else \
		( 1
		, 'output trade file: ' + outputFile + '\ntrades with multiple SSI: ' \
			+ ' '.join(tradesWithMultipleSSI)
		)

	except:
		logger.exception('processTrade():')
		return (2, '')



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



def checkOnlyOne(L):
	if len(L) != 1:
		raise ValueError('should be only one file, but {0} found'.format(len(L)))

	return L



"""
	[Function] filterFunc
	[String] directory
		=> [String] file

	Where filterFunc ([Iterator] => [Iterator]) is a filtering function
	that filters out the desired file names from a list of file names.
"""
getFileFromDirectory = lambda filterFunc, directory: \
compose(
  	lambda L: L[0]
  , checkOnlyOne
  , list
  , filterFunc
  , getFiles
)(directory)



def output(items, headers, outputFile):
	"""
	[Iterator] items
	[List] headers
	[String] outputFile
		=> [String] outputFile

	"""
	dictToValues = lambda d: \
		map(lambda h: d.get(h, ''), headers)

	return writeCsv(outputFile, map(dictToValues, items))




if __name__ == '__main__':
	import logging.config
	logging.config.fileConfig('logging.config', disable_existing_loggers=False)

	"""
	Do the following:

	1. Convert the input file to output csv file.
	2. Send notification email.
	3. Move the input file to 'processed' sub directory.
	"""
	logger.debug('main:')
	# try:
	# 	inputFile = getTradeFileFromDirectory()

	# 	trades, tradesWithMultipleSSI = \
	# 		getBociTrades(fileToLines(join(getInputDirectory(), inputFile)))

	# 	output(trades, getTradeCsvHeaders(), getOutputDirectory())

		# sendNotification( getTradeWithMultipleSSI(trades)
		# 				, output(trades, getOutputDirectory()))

	# 	shutil.move( join(getInputDirectory(), inputFile)
	# 			   , join(getInputDirectory(), 'processed', inputFile))

	# except:
	# 	logger.exception('main')
		# sendNotification('Error occurred when converting BOCI-Prudential trade file', '')


	print(processTrade(getInputDirectory(), getOutputDirectory()))