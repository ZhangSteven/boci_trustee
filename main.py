# coding=utf-8
# 
# Converts Bloomberg trade file to BOCI-Prudential trade file format and
# send notification email
# 
from boci_trustee.utility import getInputDirectory, getOutputDirectory\
						, getMailSender, getMailRecipients, getMailServer\
						, getMailTimeout, getCurrentDir
from boci_trustee.trade import getBociTrades, getTradeCsvHeaders
from boci_trustee.repo import getRepoTrades, getRepoCsvHeaders
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

	status code: 0:successful, 1:warning, -1:error
	"""
	logger.debug('processTrade(): {0}'.format(inputDir))

	try:
		inputFile = getFileFromDirectory(
						partial(filter, lambda f: f.startswith('TD') and f.endswith('.xlsx'))
					  , inputDir
					)

		trades, tradesWithMultipleSSI = \
			getBociTrades(fileToLines(join(inputDir, inputFile)))

		outputFile = output( trades
						   , getTradeCsvHeaders()
						   , join(outputDir, changeFileExtension(inputFile)))

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
		return (-1, '')



def processRepo(inputDir, outputDir):
	"""
	[String] inputDir, [String] outputDir
		=> [int] status code, [String] message

	Side effect: 
	(1) move the input file to another directory if processing successful;
	(2) produce an output csv file in the output directory;

	status code: 0:successful, 1:warning, -1:error
	"""
	logger.debug('processRepo(): {0}'.format(inputDir))

	try:
		inputFile = getFileFromDirectory(
						partial(filter, lambda f: f.startswith('REPO') and f.endswith('.xlsx'))
					  , inputDir
					)

		outputFile = output( getRepoTrades(fileToLines(join(inputDir, inputFile)))
						   , getRepoCsvHeaders()
						   , join(outputDir, changeFileExtension(inputFile)))

		shutil.move( join(inputDir, inputFile)
				   , join(inputDir, 'processed', inputFile))

		return (0, 'output repo file: ' + outputFile)

	except:
		logger.exception('processRepo():')
		return (-1, '')



def sendNotification(messageTuple1, messageTuple2):
	"""
	[Tuple] (status code, message)
	[Tuple] (status code, message)
	=> no return value
	
	side effect: send notification email to recipients.
	"""
	status01, message01 = messageTuple1
	status02, message02 = messageTuple2

	subject = 'Successful: 60001 trade and repo file conversion' \
				if (status01, status02) == (0, 0) else \
				'Warning: 60001 trade and repo file conversion: Broker with Muitple SSI' \
				if (status01, status02) in [(0, 1), (1, 0)] else \
				'Error: 60001 trade and repo file conversion'

	body = message01 + '\n\n' + message02

	# sendMail( body, subject, getMailSender(), getMailRecipients()\
	# 		, getMailServer(), getMailTimeout())

	print('send mail:', subject, body) # for debugging only



def changeFileExtension(filename):
	L = filename.split('.')
	if len(L) < 2:
		logger.error('changeFileExtension(): invalid filename {0}'.format(filename))
		raise ValueError

	return '.'.join(L[0:-1] + ['csv'])




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
	1. Search for trade and repo file in the input directory;
	2. Process them and save the results to csv files;
	3. Send notification email.

	"""
	logger.debug('main:')

	sendNotification( processTrade(getInputDirectory(), getOutputDirectory())
					, processRepo(getInputDirectory(), getOutputDirectory()))