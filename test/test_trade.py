# coding=utf-8
# 

import unittest2
from boci_trustee.utility import getCurrentDir
from boci_trustee.main import getBociTrades
from steven_utils.excel import fileToLines
from os.path import join



class TestTrade(unittest2.TestCase):

	def __init__(self, *args, **kwargs):
		super(TestTrade, self).__init__(*args, **kwargs)



	def testTrade1(self):
		inputFile = join(getCurrentDir(), 'samples', 'sample_trade1.xlsx')
		trades, _ = getBociTrades(fileToLines(inputFile))
		t = list(trades)[1]
		self.assertEqual('12345678', t['Account'])
		self.assertEqual('BK5JS96', t['SEDOL'])
		self.assertEqual('XS1813551584', t['ISIN'])
		self.assertEqual('HOPSON DEVELOP', t['Name'])
		self.assertEqual('S', t['TranType'])
		self.assertEqual(1996000, t['Quantity'])
		self.assertEqual('07/10/2020', t['TradeDate'])
		self.assertEqual('09/10/2020', t['SettlementDate'])
		self.assertEqual('USD', t['Currency'])
		self.assertEqual(100.58, t['Price'])
		self.assertEqual(42415, t['AccurredInterest'])
		self.assertEqual(2049991.80, t['SettlementAmount'])
		self.assertEqual(0, t['Commission'])
		self.assertEqual('', t['StampDuty'])
		self.assertEqual('', t['TransactionLevy'])
		self.assertEqual('', t['ClearingFee'])
		self.assertEqual('', t['SalesTax'])
		self.assertEqual('', t['HongKongCCASSFee'])
		self.assertEqual('281305', t['TradeReferenceNumber'])
		self.assertEqual('94589', t['BrokerCode'])
		self.assertEqual('GOLDMAN SACHS', t['BrokerName'])



	def testTrade2(self):
		inputFile = join(getCurrentDir(), 'samples', 'sample_trade2.xlsx')
		_, tradesWithMultipleSSI = getBociTrades(fileToLines(inputFile))
		self.assertEqual(['281305', '282617', '283003'], tradesWithMultipleSSI)