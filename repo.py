# coding=utf-8
# 
# Read Bloomberg repo XML trade file and repo XML rerate file, then convert them
# to BOCI-Prudential repo trade file format.
# 
# It assumes repo master information is already in the repo database.
# 
from aim_xml.add_header import isRepoTrade, isRepoRerate
from repo_data.data import initializeDatastore, getRepo
from steven_utils.file import getFiles
from boci_trustee.utility import getCurrentDir
from toolz.functoolz import compose
from functools import partial
import logging
logger = logging.getLogger(__name__)



"""
1 get repo trade file and repo rerate file;

2 convert trade file to repo trade data (iterable);

3 convert rerate file to repo rerate data (iterable);

4 combine them and write to output csv

5 notify user about the result, with output csv as attachment 
(no repo, success, error)
"""

def getRepoTradeFiles(directory):
	"""
	[String] => [List] ([String] trade file)

	If there is no repo trade file in the directory, return an empty list.
	If there is one, return a list of one file.
	If there are more than one, raise exception.
	"""
	return []



def getRepoRerateFiles(directory):
	"""
	[String] => [List] ([String] rerate file)

	If there is no repo rerate file in the directory, return an empty list.
	If there is one, return a list of one file.
	If there are more than one, raise exception.
	"""
	return []



def readRepoTradeFile(file):
	"""
	[String] file => 
		( [Iterable] ([Dictionary] repo trade data)
		, [Iterable] ([Dictionary] repo close trade data)
		, [Iterable] ([Dictionary] repo cancel trade data)
		)
	"""
	return []



def readRepoRerateFile(file):
	"""
	[String] file => [Iterable] ([Dictionary] repo rerate data)
	"""
	return []



def bociTrade(tradeInfo):
	"""
	[Dictionary] repo trade data => [Dictionary] boci repo trade data
	"""
	return {}



def bociClose(closeInfo):
	"""
	[Dictionary] repo close data => [Dictionary] boci repo close data
	"""
	return {}



def bociCancel(cancelInfo):
	"""
	[Dictionary] repo cancel data => [Dictionary] boci repo cancel data
	"""
	return {}



def bociRerate(rerateInfo):
	"""
	[Dictionary] repo rerate data => [Dictionary] boci repo rerate data
	"""
	return {}






