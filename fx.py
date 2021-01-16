# coding=utf-8
# 
# Converts Bloomberg THRP FX trade file to BOCI-Prudential fx trade file 
# format.
# 
from boci_trustee.trade import toDateTimeString as toDateTimeStringFromFloat \
							, convert, toStringIfFloat
from toolz.functoolz import compose
from functools import partial
from datetime import datetime
import logging
logger = logging.getLogger(__name__)



toDateTimeString = lambda x: \
	datetime.strptime(x, '%d/%m/%y').strftime('%d/%m/%Y') \
	if isinstance(x, str) else toDateTimeStringFromFloat(x)



def ticketToTrade(ticket):
	"""
	[Dictionary] FX trade ticket (Bloomberg)
		=> [Dictionary] BOCI Prudential fx record
	"""
	getBuySellCurrency = lambda ticket: \
	compose(
		lambda L: (L[0], L[1]) if ticket['B/S'] == 'B' else (L[1], L[0])
	  , lambda name: name.split()[0].split('/')
	)(ticket['Shrt Name'])

	
	getBuySellAmount = lambda ticket: \
	compose(
		lambda t: \
			(ticket['Amount Pennies'], ticket['Amount Pennies']*ticket['Price']) \
			if t[0] == ticket['Crcy'] else \
			(ticket['Amount Pennies']*ticket['Price'], ticket['Amount Pennies'])
			
	  , getBuySellCurrency
	)(ticket)


	t = {}
	t['Portfolio Code'] = ticket['Fund']
	t['Settlement Account'] = ''
	t['FXS Contract No.'] = toStringIfFloat(ticket['Tkt #'])
	t['Spot Deal Ref No.'] = ''
	t['Trade Date'] = toDateTimeString(ticket['As of Dt'])
	t['Settlement Date'] = toDateTimeString(ticket['Stl Date'])
	t['Transaction Type'] = ticket['FX Trade Deal Type']
	t['Exchange Code'] = ''
	t['Client Buy Currency'], t['Client Sell Currency'] = \
		getBuySellCurrency(ticket)
	t['Client Buy Amount'], t['Client Sell Amount'] = \
		getBuySellAmount(ticket)

	t['Exchange Rate'] = ''
	t['Source Application ID'] = ''
	t['Broker Code'] = ''
	t['Class Code'] = ''

	return t



# [Iterable] lines => [Iterable] ([Dictionary] fx record)
getFXTrades = partial(convert, ticketToTrade)



getFXCsvHeaders = lambda : \
	( 'Portfolio Code', 'Settlement Account', 'FXS Contract No.', 'Spot Deal Ref No.'
	, 'Trade Date', 'Settlement Date', 'Transaction Type', 'Exchange Code'
	, 'Client Buy Currency', 'Client Buy Amount', 'Client Sell Currency', 'Client Sell Amount'
	, 'Exchange Rate', 'Source Application ID', 'Broker Code', 'Class Code'
	)