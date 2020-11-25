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



def processTrade(inputFile, inputDir, outputDir):
	"""
	[String] inputFile, [String] inputDir, [String] outputDir
		=> [int] status code, [String] message

	Side effect: 
	(1) move the input file to another directory if processing successful;
	(2) produce an output csv file in the output directory;

	status code: -1:error, 0:successful, 1:warning
	"""
	logger.debug('processTrade(): {0}'.format(inputFile))

	try:
		trades, tradesWithMultipleSSI = \
			getBociTrades(fileToLines(join(inputDir, inputFile)))

		outputFile = output( trades
						   , getTradeCsvHeaders()
						   , join(outputDir, changeFileExtension(inputFile)))

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



def processRepo(inputFile, inputDir, outputDir):
	"""
	[String] inputFile, [String] inputDir, [String] outputDir
		=> [int] status code, [String] message

	Side effect: 
	(1) move the input file to another directory if processing successful;
	(2) produce an output csv file in the output directory;

	status code: -1: error, 0: successful
	"""
	logger.debug('processRepo(): {0}'.format(inputDir))

	try:
		outputFile = output( getRepoTrades(fileToLines(join(inputDir, inputFile)))
						   , getRepoCsvHeaders()
						   , join(outputDir, changeFileExtension(inputFile)))

		return (0, 'output repo file: ' + outputFile)

	except:
		logger.exception('processRepo():')
		return (-1, '')



def sendNotification(fileType, statusCode, message):
	"""
	[String] fileType
	[Int] statusCode
	[String] message
		=> no return value
	
	side effect: send notification email to recipients.
	"""
	subject = 'Successful: 60001 {0} file conversion'.format(fileType) \
				if statusCode == 0 else \
				'Warning: 60001 {0} file conversion, check details below'.format(fileType) \
				if statusCode == 1 else \
				'Error: 60001 {0} file conversion'.format(fileType)

	sendMail( message, subject, getMailSender(), getMailRecipients()\
			, getMailServer(), getMailTimeout())

	# print('send mail: {0}\n{1}'.format(subject, message)) # for debugging only



def changeFileExtension(filename):
	L = filename.split('.')
	if len(L) < 2:
		logger.error('changeFileExtension(): invalid filename {0}'.format(filename))
		raise ValueError

	return '.'.join(L[0:-1] + ['csv'])



"""
	[String] input directory
	[String] file type
		=> [String] input file
	
	Search for the input file based on file type.
"""
getInputFiles = lambda inputDir, fileType: \
compose(
	list
  , partial(filter, lambda f: f.startswith('TD') and f.endswith('.xlsx')) \
	if fileType == 'trade' else \
	partial(filter, lambda f: f.startswith('REPO') and f.endswith('.xlsx'))
  , getFiles
)(inputDir)



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
	
	import argparse
	parser = argparse.ArgumentParser(description='Process 60001 THRP File for BOCI-Prudential')
	parser.add_argument( 'fileType', metavar='file type', type=str
					   , help='THRP trade file type (trade or repo)')

	"""
		To handle THRP trade file, do

		$python main.py trade

		To handle THRP repo trade file, do

		$python main.py repo
	"""
	import sys
	fileType = parser.parse_args().fileType
	
	if not fileType in ['trade', 'repo']:
		logger.error('invalid file type: {0}'.format(fileType))
		sys.exit(1)

	files = getInputFiles(getInputDirectory(), fileType)
	
	if len(files) == 0:
		logger.debug('no input {0} file found'.format(fileType))
		sys.exit(0)

	elif len(files) > 1:
		logger.error('{0} files found for {1}'.format(len(files), fileType))
		sys.exit(1)
	
	else:
		sendNotification( fileType
						, *processTrade( files[0]
									   , getInputDirectory()
									   , getOutputDirectory())) \
		if fileType == 'trade' else \
		sendNotification( fileType
						, *processRepo( files[0]
									  , getInputDirectory()
									  , getOutputDirectory()))

		shutil.move( join(getInputDirectory(), files[0])
				   , join(getInputDirectory(), 'processed', files[0]))