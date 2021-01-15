# coding=utf-8
# 

import unittest2
from boci_trustee.utility import getCurrentDir
from boci_trustee.position import getValuationDataFromFile \
								, cashReconPosition, bondReconPosition
from functools import partial
from os.path import join



class TestPosition(unittest2.TestCase):

	def __init__(self, *args, **kwargs):
		super(TestPosition, self).__init__(*args, **kwargs)



	def testCashReconPosition(self):
		inputFile = join(getCurrentDir(), 'samples', 'sample 2021-01-12.xls')
		date, _, _, bondPositions, cashPositions = \
			getValuationDataFromFile(inputFile)

		cashReconPositions = list(map( partial(cashReconPosition, date)
									 , cashPositions))
		bondReconPositions = list(map( partial(bondReconPosition, date)
									 , bondPositions))
		self.assertEqual(1, len(cashReconPositions))
		self.assertEqual(15, len(bondReconPositions))
		self.verifyCashReconPosition(cashReconPositions[0])
		self.verifyBondReconPosition(bondReconPositions[14])



	def verifyCashReconPosition(self, position):
		self.assertEqual(5, len(position))
		self.assertEqual('Short Term Bond Fund', position['portfolio'])
		self.assertEqual('', position['custodian'])
		self.assertEqual('2021-01-12', position['date'])
		self.assertEqual('USD', position['currency'])
		self.assertEqual(70831279.39, position['balance'])



	def verifyBondReconPosition(self, position):
		self.assertEqual(9, len(position))
		self.assertEqual('Short Term Bond Fund', position['portfolio'])
		self.assertEqual('', position['custodian'])
		self.assertEqual('2021-01-12', position['date'])
		self.assertEqual('', position['geneva_investment_id'])
		self.assertEqual('USG8850LAB65', position['ISIN'])
		self.assertEqual('', position['bloomberg_figi'])
		self.assertEqual( 'THREE GORGES FINANCE I CAYMAN ISLANDS LTD 2.3% S/A 02JUN2021 REGS'
						, position['name'])
		self.assertEqual('USD', position['currency'])
		self.assertEqual(6275000, position['quantity'])