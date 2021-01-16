# coding=utf-8
# 

import unittest2
from boci_trustee.utility import getCurrentDir
from boci_trustee.fx import getFXTrades
from steven_utils.excel import fileToLines
from os.path import join



class TestFX(unittest2.TestCase):

	def __init__(self, *args, **kwargs):
		super(TestFX, self).__init__(*args, **kwargs)



	def testFx(self):
		inputFile = join(getCurrentDir(), 'samples', 'sample_fx.xlsx')
		fxRecords = list(getFXTrades(fileToLines(inputFile)))
		self.assertEqual(8, len(fxRecords))
		self.verifyFX1(fxRecords[2])
		self.verifyFX2(fxRecords[1])



	def verifyFX1(self, position):
		self.assertEqual(position['Portfolio Code'], 'TEST')
		self.assertEqual(position['Settlement Account'], '')
		self.assertEqual(position['FXS Contract No.'], '296310')
		self.assertEqual(position['Spot Deal Ref No.'], '')
		self.assertEqual(position['Trade Date'], '28/12/2020')
		self.assertEqual(position['Settlement Date'], '30/12/2020')
		self.assertEqual(position['Transaction Type'], 'SPOT')
		self.assertEqual(position['Exchange Code'], '')
		self.assertEqual(position['Client Buy Currency'], 'HKD')
		self.assertEqual(position['Client Sell Currency'], 'USD')
		self.assertAlmostEqual(position['Client Buy Amount'], 10000.0645, 4)
		self.assertAlmostEqual(position['Client Sell Amount'], 1289.98136)



	def verifyFX2(self, position):
		self.assertEqual(position['Portfolio Code'], 'TEST')
		self.assertEqual(position['Settlement Account'], '')
		self.assertEqual(position['FXS Contract No.'], '296302')
		self.assertEqual(position['Spot Deal Ref No.'], '')
		self.assertEqual(position['Trade Date'], '28/12/2020')
		self.assertEqual(position['Settlement Date'], '01/02/2021')
		self.assertEqual(position['Transaction Type'], 'FORWARD')
		self.assertEqual(position['Exchange Code'], '')
		self.assertEqual(position['Client Buy Currency'], 'USD')
		self.assertEqual(position['Client Sell Currency'], 'HKD')
		self.assertAlmostEqual(position['Client Buy Amount'], 50000)
		self.assertAlmostEqual(position['Client Sell Amount'], 387480)
