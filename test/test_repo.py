# coding=utf-8
# 

import unittest2
from boci_trustee.utility import getCurrentDir
from boci_trustee.repo import getRepoTrades
from utils.excel import fileToLines
from utils.iter import firstOf
from os.path import join



class TestRepo(unittest2.TestCase):

	def __init__(self, *args, **kwargs):
		super(TestRepo, self).__init__(*args, **kwargs)



	def test_convert(self):
		inputFile = join(getCurrentDir(), 'samples', 'sample_repo_trade1.xlsx')
		trades = list(getRepoTrades(fileToLines(inputFile)))
		self.assertEqual(60, len(trades))
		self.verifyTrade1(firstOf( lambda t: t['Amount'] == 993120 \
												and t['Col_ISIN'] == 'USY0606WCA63' 
								 , trades))
		self.verifyTrade2(firstOf( lambda t: t['Col_ISIN'] == 'XS2211674143' 
								 , trades))
		self.verifyTrade3(firstOf( lambda t: t['Cust_ref'] == '218783' 
								 , trades))
		self.verifyTrade4(firstOf( lambda t: t['Amount'] == 3343040 \
												and t['Col_ISIN'] == 'XS2008565116' 
								 , trades))



	def verifyTrade1(self, trade):
		self.assertEqual('666666', trade['Portfolio_code'])
		self.assertEqual('REPO', trade['Txn_type'])
		self.assertEqual('Close', trade['Txn_sub_type'])
		self.assertEqual('USD', trade['Loan_ccy'])
		self.assertEqual('02/03/2020', trade['Mature_date'])
		self.assertEqual(1200000, trade['Col_Qty'])
		self.assertEqual('SOCG-REPO', trade['Broker'])
		self.assertEqual('226819', trade['Cust_ref'])



	def verifyTrade2(self, trade):
		self.assertEqual('666666', trade['Portfolio_code'])
		self.assertEqual('REPO', trade['Txn_type'])
		self.assertEqual('Open', trade['Txn_sub_type'])
		self.assertEqual('29/10/2020', trade['Trade_date'])
		self.assertEqual('30/10/2020', trade['Settle_date'])
		self.assertEqual('04/01/2021', trade['Mature_date'])
		self.assertEqual('USD', trade['Loan_ccy'])
		self.assertEqual(3209280, trade['Amount'])
		self.assertEqual('30/10/2020', trade['Eff_date'])
		self.assertEqual(1.0, trade['Int_rate'])
		self.assertEqual('ACT/360', trade['Int_mode'])
		self.assertEqual(4000000, trade['Col_Qty'])
		self.assertEqual('SOCG-REPO', trade['Broker'])
		self.assertEqual('285050', trade['Cust_ref'])



	def verifyTrade3(self, trade):
		self.assertEqual('666666', trade['Portfolio_code'])
		self.assertEqual('REPO', trade['Txn_type'])
		self.assertEqual('Change Rate', trade['Txn_sub_type'])
		self.assertEqual('08/07/2020', trade['Eff_date'])
		self.assertEqual(1.2, trade['Int_rate'])
		self.assertEqual('ACT/360', trade['Int_mode'])



	def verifyTrade4(self, trade):
		self.assertEqual('666666', trade['Portfolio_code'])
		self.assertEqual('REPO', trade['Txn_type'])
		self.assertEqual('Open', trade['Txn_sub_type'])
		self.assertEqual('30/10/2020', trade['Trade_date'])
		self.assertEqual('02/11/2020', trade['Settle_date'])
		self.assertEqual('31/12/2049', trade['Mature_date'])
		self.assertEqual('USD', trade['Loan_ccy'])
		self.assertEqual('02/11/2020', trade['Eff_date'])
		self.assertEqual(1.0, trade['Int_rate'])
		self.assertEqual('ACT/360', trade['Int_mode'])
		self.assertEqual(4000000, trade['Col_Qty'])
		self.assertEqual('SOCG-REPO', trade['Broker'])
		self.assertEqual('285215', trade['Cust_ref'])