#!/usr/bin/env python
import os
import requests
import json
import pprint
import pandas as pd 
import numpy as np
from datetime import datetime
from time import sleep
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
import subprocess

#Email
import smtplib
from email.message import EmailMessage


# [ START ] -  Functions -------------------------------------------------------------------------------

def Insert_DF_inGsheets(gsheetindex,df):
	#Activate Sheet
	Name = sh.get_worksheet(gsheetindex) #-> 0 - first sheet, 1 - second sheet etc.
	# APPEND DATA TO SHEET
	your_dataframe = pd.DataFrame(df)
	set_with_dataframe(Name, your_dataframe) #-> THIS EXPORTS YOUR DATAFRAME TO THE GOOGLE SHEET


def Read_DF_inGsheets(gsheetindex):
	Name = sh.get_worksheet(gsheetindex) #-> 0 - first sheet, 1 - second sheet etc.	
	Table = Name.get_all_records() # list of dictionaries
	dfRead = pd.DataFrame(Table)
	return dfRead

def InitDataFrames(StartupFile,path):
	df_Init = pd.read_excel(StartupFile)
	df_Init.to_excel(path, index=False)



def Call_EwonAPI(ProjectName,talk2M_DevID,talk2M_TokenID,LastTransactionID_Recorded,YearFolder):
	# Function mission: Call Selected eWon by following the last API Talk2m Transaction request.

	try:

		###### Ewon status ONline #########
		ProjectStatusDF = pd.read_csv('/home/pi/Desktop/DataHistorisationAPP/IoT Gateway Status/Offline_Table.csv', sep=',')
		df_fname = ProjectStatusDF.loc[ProjectStatusDF['Project'] == ProjectName]
		dfrow = df_fname['No'].values[0] - 1
		ProjectStatusDF.at[dfrow,"Offline Status"] = 'NO'
		ProjectStatusDF.to_csv('/home/pi/Desktop/DataHistorisationAPP/IoT Gateway Status/Offline_Table.csv' ,sep=',', index=False , encoding='utf-8')
		EwonStatusOK = 1
		#####################################

		# -x-x-x-x-x-x-x-x--x-x-x-x-x--x-x-x-x-x-x-  HRS   -x-x-x-x-x-x-x--x-x-x-x-x-x-x-x-x-x-x-x-x-x
		MoreNewData = 0
		Project_Dictionary_DF= pd.read_csv('/home/pi/Desktop/DataHistorisationAPP/Dictionaries/Dicc_' + ProjectName + '.csv', sep=',')
		Project_Dictionary = Project_Dictionary_DF.set_index('EwonID').T.to_dict('list')

		TotalDimension = 0
		print('---------------------------------------------------------------------------------------------------------')
		print('\n')
		print('[ McView MSG ] - Data Wave for ' + ProjectName +  ' is about to start...')
		print('\n')

		#XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
		# First CALL APIs Datamailbox & Google  -------------------------------------------------------------------------------

		# Sync DATA POST
		Authentication = {'t2mdevid': talk2M_DevID,'t2mtoken': talk2M_TokenID, 'createTransaction':'true','lastTransactionId':LastTransactionID_Recorded}
		respuesta = requests.post('https://data.talk2m.com/syncdata', data=Authentication)
		respuestajson = respuesta.json()



		print('-----------------------------------')
		print('McView Data Historisation Service')
		print('-----------------------------------')
		#Transaction ID Data mailbox
		TransactionID = respuestajson.get('transactionId')
		NewTransactionID_Recorded = str(TransactionID)
		print('Project Name: ' + str(ProjectName))
		print('Transaction ID: ' + str(TransactionID))

		#Transactionsheet = sh.get_worksheet(0)
		#Transactionsheet.update_cell(1,2, TransactionID)


		###### NEW Method for store locally the transaction #########
		Main_DataFrameIntern = pd.read_csv('/home/pi/Desktop/DataHistorisationAPP/Setup_McViewDataHistorisation.csv', sep=',')
		df_fname = Main_DataFrameIntern.loc[Main_DataFrameIntern['ProjectName'] == ProjectName]
		dfrow = df_fname['No'].values[0] - 1
		Main_DataFrameIntern.at[dfrow,"LastTransaction"] = NewTransactionID_Recorded
		Main_DataFrameIntern.to_csv('/home/pi/Desktop/DataHistorisationAPP/Setup_McViewDataHistorisation.csv' ,sep=',', index=False , encoding='utf-8')
		#########################################################





		#Flag.. More Data Available?
		FLAGMoreData = respuestajson.get('moreDataAvailable')
		print('More Data Available: ' + str(FLAGMoreData))
		if FLAGMoreData == True:
			MoreNewData = 1
		#Ewon tags inside Reponse 
		ewonTags = respuestajson.get('ewons')[0].get('tags')
		range_ewontagsinResponse = len(ewonTags)
		contador = range_ewontagsinResponse
		print('Response Lenght: '+ str(range_ewontagsinResponse))
		print('-----------------------------------')
		print('\n')
		print('---------------------------------------------------------------------------------------------------------')


		conca = 1

		for x in range(0,range_ewontagsinResponse):
			#print('--------------------')
			ix = respuestajson.get('ewons')[0].get('tags')[x]
			Mcview_TagID = ix['ewonTagId']


			if 'history' in ix:

				IDArray = Project_Dictionary[Mcview_TagID]		# Read Array Id MCview
				IndexNumber = IDArray[0]						# Read Index Number ID
				TabName_Gsheet = IDArray[1]						# Read Tab name Id Mcview
				DescriptionTag = IDArray[2]						# Read Description Tag

				newdata = ix['history']
				dfbrut = pd.DataFrame(newdata)
				df = dfbrut.loc[:,['value', 'date']]
				df['TagId'] = Mcview_TagID
				df['date'] = pd.to_datetime(df.date)
				df['Hour'] = df.date.dt.hour
				df['Month'] = df.date.dt.month
				df['Extrainfo'] = DescriptionTag
				df['Tagname'] = TabName_Gsheet
				df.rename(columns = {'value':'Value'}, inplace=True)
				df.rename(columns = {'date':'TimeStr'}, inplace=True)
				custom_sort = ['Value', 'TimeStr', 'TagId', 'Hour', 'Tagname', 'Month','Extrainfo']
				df = df[custom_sort]


				TimeValue = df['TimeStr'].iloc[0]
				print(ProjectName + ' Mcview TagID ' + str(Mcview_TagID) + ' : Loop # ' + str(contador) + ' : ' + str(TabName_Gsheet) + ' : ' + str(TimeValue))

				if IndexNumber != 999:		
					if conca == 1:

						dfStorage = pd.read_csv('/home/pi/Desktop/DataHistorisationAPP/' + 'DataStorage/' + ProjectName + '/' + 'Year ' + str(YearFolder) + '/' + TabName_Gsheet + '.csv' , sep=',')
						dfnewStorage = pd.concat([dfStorage,df])
						dfnewStorage.to_csv('/home/pi/Desktop/DataHistorisationAPP/' + 'DataStorage/' + ProjectName + '/' + 'Year ' + str(YearFolder) + '/' + TabName_Gsheet + '.csv' ,sep=',', index=False , encoding='utf-8')


						sleep(0.1)


			contador = contador - 1


		print('---------------------------------------------------------------------------------------------------------')
		# More Data LOOP -------------------------------------------------------------------------------

		### More Data Available
		while MoreNewData == 1:
			TotalDimension = 0
			print('\n')
			print('\n')
			print("[ McView MSG ] - Waiting for the New Request....")
			print('\n')
			sleep(1) #esperando 1 min 10 segundos para el siguiente request			
			

			# Sync DATA POST
			Authentication = {'t2mdevid': talk2M_DevID,'t2mtoken': talk2M_TokenID, 'createTransaction':'true','lastTransactionId':NewTransactionID_Recorded}
			respuesta = requests.post('https://data.talk2m.com/syncdata', data=Authentication)
			respuestajson = respuesta.json()



			print('-----------------------------------')
			print('McView Data Historisation Service')
			print('-----------------------------------')
			#Transaction ID Data mailbox
			TransactionID = respuestajson.get('transactionId')
			NewTransactionID_Recorded = str(TransactionID)
			print('Project Name: ' + str(ProjectName))
			print('Transaction ID: ' + str(TransactionID))


			###### NEW Method for store locally the transaction #########
			Main_DataFrameIntern = pd.read_csv('/home/pi/Desktop/DataHistorisationAPP/Setup_McViewDataHistorisation.csv', sep=',')
			df_fname = Main_DataFrameIntern.loc[Main_DataFrameIntern['ProjectName'] == ProjectName]
			dfrow = df_fname['No'].values[0] - 1
			Main_DataFrameIntern.at[dfrow,"LastTransaction"] = NewTransactionID_Recorded
			Main_DataFrameIntern.to_csv('/home/pi/Desktop/DataHistorisationAPP/Setup_McViewDataHistorisation.csv' ,sep=',', index=False , encoding='utf-8')
			#########################################################


			#Flag.. More Data Available?
			FLAGMoreData = respuestajson.get('moreDataAvailable')
			print('More Data Available: ' + str(FLAGMoreData))
			if FLAGMoreData == True:
				MoreNewData = 1
			#Ewon tags inside Reponse 
			ewonTags = respuestajson.get('ewons')[0].get('tags')
			range_ewontagsinResponse = len(ewonTags)
			contador = range_ewontagsinResponse
			print('Response Lenght: '+ str(range_ewontagsinResponse))
			print('-----------------------------------')
			print('\n')
			print('---------------------------------------------------------------------------------------------------------')


			conca = 1

			for x in range(0,range_ewontagsinResponse):
				#print('--------------------')
				ix = respuestajson.get('ewons')[0].get('tags')[x]
				#print('\n')
				Mcview_TagID = ix['ewonTagId']


				if 'history' in ix:

					IDArray = Project_Dictionary[Mcview_TagID]		# Read Array Id MCview
					IndexNumber = IDArray[0]						# Read Index Number ID
					TabName_Gsheet = IDArray[1]						# Read Tab name Id Mcview
					DescriptionTag = IDArray[2]						# Read Description Tag

					newdata = ix['history']
					dfbrut = pd.DataFrame(newdata)
					df = dfbrut.loc[:,['value', 'date']]
					df['TagId'] = Mcview_TagID
					df['date'] = pd.to_datetime(df.date)
					df['Hour'] = df.date.dt.hour
					df['Month'] = df.date.dt.month
					df['Extrainfo'] = DescriptionTag
					df['Tagname'] = TabName_Gsheet
					df.rename(columns = {'value':'Value'}, inplace=True)
					df.rename(columns = {'date':'TimeStr'}, inplace=True)
					custom_sort = ['Value', 'TimeStr', 'TagId', 'Hour', 'Tagname', 'Month','Extrainfo']
					df = df[custom_sort]


					TimeValue = df['TimeStr'].iloc[0]
					print(ProjectName + ' Mcview TagID ' + str(Mcview_TagID) + ' : Loop # ' + str(contador) + ' : ' + str(TabName_Gsheet) + ' : ' + str(TimeValue))

					if IndexNumber != 999:		
						if conca == 1:

							dfStorage = pd.read_csv('/home/pi/Desktop/DataHistorisationAPP/' + 'DataStorage/' + ProjectName + '/' + 'Year ' + str(YearFolder) + '/' + TabName_Gsheet + '.csv' , sep=',')
							dfnewStorage = pd.concat([dfStorage,df])
							dfnewStorage.to_csv('/home/pi/Desktop/DataHistorisationAPP/' + 'DataStorage/' + ProjectName + '/' + 'Year ' + str(YearFolder) + '/' + TabName_Gsheet + '.csv' ,sep=',', index=False , encoding='utf-8')

							sleep(0.1)


				contador = contador - 1


			if FLAGMoreData != True:
				MoreNewData = 0

		print('---------------------------------------------------------------------------------------------------------')

	except:
		print('-----------------------------------')
		print('\n')
		print('[ ERROR 01 ] - Error when establishing the connection with eWon ' + ProjectName)
		MoreNewData = 0
		
		###### Ewon status Offline #########
		ProjectStatusDF = pd.read_csv('/home/pi/Desktop/DataHistorisationAPP/IoT Gateway Status/Offline_Table.csv', sep=',')
		df_fname = ProjectStatusDF.loc[ProjectStatusDF['Project'] == ProjectName]
		dfrow = df_fname['No'].values[0] - 1
		ProjectStatusDF.at[dfrow,"Offline Status"] = 'YES'
		ProjectStatusDF.to_csv('/home/pi/Desktop/DataHistorisationAPP/IoT Gateway Status/Offline_Table.csv' ,sep=',', index=False , encoding='utf-8')
		EwonStatusOK = 0
		#####################################

		print('\n')
		pass


####################################### END Functions #######################################



# [ START ] - Init Section ----------------------------------------------------------------------------

global LOOPApi
global MoreNewData
global EwonStatusOK

LoopApi = 0
MoreNewData = 0
EwonStatusOK = 0


print('\n')
print('#########################################################')
print('#########################################################')
print('#############                               #############')
print('#############          McView Server        #############')
print('#############       Data Historisation      #############')
print('#############           Version 1.1         #############')
print('#############                               #############')
print('#########################################################')
print('#########################################################')
print('\n')


####################################### [ END ] - Init Section #######################################


# [ START ] - Main LOOP ----------------------------------------------------------------------------

while True:

	CycleStartTime = datetime.now()
	Main_DataFrame = pd.read_csv('/home/pi/Desktop/DataHistorisationAPP/Setup_McViewDataHistorisation.csv', sep=',')
	df_f1 = Main_DataFrame.loc[Main_DataFrame['ActivateService'].isin(['YES','Yes','yes'])]
	dfLenght = len(df_f1)

	for x in range(dfLenght):
		Call_EwonAPI(df_f1['ProjectName'].values[x], df_f1['t2mDevID'].values[x], df_f1['t2mtokenID'].values[x], df_f1['LastTransaction'].values[x], df_f1['PRM_YearLogged'].values[x])
		sleep(3)

		# Run Shel Script for Update McView Sharepoint Folders.
		try:

			if (df_f1['ProjectName'].values[x] == 'HRS-H2M') and (EwonStatusOK == 1):
				print('\n')
				print('----------------------------------------------------------------------')
				subprocess.run("sh /home/pi/Desktop/DataHistorisationAPP/McView-DataServer/syncHRSH2M.sh", shell=True)
				print('----------------------------------------------------------------------')
				print('\n')
				print("[ McView MSG ] - HRS-H2M Data: SharePoint Folder Updated")
				print('\n')
				sleep(1)

			if (df_f1['ProjectName'].values[x] == 'HRS-CNR') and (EwonStatusOK == 1):
				print('\n')
				print('----------------------------------------------------------------------')
				subprocess.run("sh /home/pi/Desktop/DataHistorisationAPP/McView-DataServer/syncHRSCNR.sh", shell=True)
				print('----------------------------------------------------------------------')
				print('\n')
				print("[ McView MSG ] - HRS-CNR Data: Project SharePoint Folder Updated")
				print('\n')
				sleep(1)

			if (df_f1['ProjectName'].values[x] == 'HRS-IP1') and (EwonStatusOK == 1):
				print('\n')
				print('----------------------------------------------------------------------')
				subprocess.run("sh /home/pi/Desktop/DataHistorisationAPP/McView-DataServer/syncHRSIP1.sh", shell=True)
				print('----------------------------------------------------------------------')
				print('\n')
				print("[ McView MSG ] - HRS-IP1 Data: SharePoint Folder Updated")
				print('\n')
				sleep(1)

			if (df_f1['ProjectName'].values[x] == 'HRS-APEX') and (EwonStatusOK == 1):
				print('\n')
				print('----------------------------------------------------------------------')
				subprocess.run("sh /home/pi/Desktop/DataHistorisationAPP/McView-DataServer/syncHRSAPEX.sh", shell=True)
				print('----------------------------------------------------------------------')
				print('\n')
				print("[ McView MSG ] - HRS-APEX Data: SharePoint Folder Updated")
				print('\n')
				sleep(1)

			if (df_f1['ProjectName'].values[x] == 'HRS1-SMTAG') and (EwonStatusOK == 1):
				print('\n')
				print('----------------------------------------------------------------------')
				subprocess.run("sh /home/pi/Desktop/DataHistorisationAPP/McView-DataServer/syncHRS1SMTAG.sh", shell=True)
				print('----------------------------------------------------------------------')
				print('\n')
				print("[ McView MSG ] - HRS1-SMTAG Data: SharePoint Folder Updated")
				print('\n')
				sleep(1)

			if (df_f1['ProjectName'].values[x] == 'HRS2-SMTAG') and (EwonStatusOK == 1):
				print('\n')
				print('----------------------------------------------------------------------')
				subprocess.run("sh /home/pi/Desktop/DataHistorisationAPP/McView-DataServer/syncHRS2SMTAG.sh", shell=True)
				print('----------------------------------------------------------------------')
				print('\n')
				print("[ McView MSG ] - HRS2-SMTAG Data: SharePoint Folder Updated")
				print('\n')
				sleep(1)

			if (df_f1['ProjectName'].values[x] == 'HRS-FAHYENCE') and (EwonStatusOK == 1):
				print('\n')
				print('----------------------------------------------------------------------')
				subprocess.run("sh /home/pi/Desktop/DataHistorisationAPP/McView-DataServer/syncHRSFAHYENCE.sh", shell=True)
				print('----------------------------------------------------------------------')
				print('\n')
				print("[ McView MSG ] - HRS-FAHYENCE Data: SharePoint Folder Updated")
				print('\n')
				sleep(1)

		except:
			print('\n')
			print("[ ERROR 02 ] - Data Historisation: Sharepoint Syncro Failure")


	# Run Python Script "Historique Distributions" --------------------------------------------------------------
	try:
		print('\n')
		print("[ McView MSG ] - Historique Distributions: Preparing Update...")
		print('\n')
		print('----------------------------------------------------------------------')
		subprocess.run("python3 /home/pi/Desktop/DataHistorisationAPP/McView-DataServer/HistoriqueDistributions.py", shell=True )
		print('----------------------------------------------------------------------')
		print('\n')
		sleep(1)
		# Update SharePoint Folder
		print('----------------------------------------------------------------------')
		subprocess.run("sh /home/pi/Desktop/DataHistorisationAPP/McView-DataServer/syncReportHistoriqueDistributions.sh", shell=True)
		print('----------------------------------------------------------------------')
		print('\n')
		print("[ McView MSG ] - Historique Distributions: SharePoint Folder Updated")

	except:
		print('\n')
		print("[ ERROR 03 ] - Historique Distributions: Sharepoint Syncro Failure")

	
	# Run Shel File to update eWons Offline Table to Sharepoint. -------------------------------------------------
	try:
		print('\n')
		print("[ McView MSG ] - Update Sharepoint Folder /IoT Gateway Status: Preparing Update...")
		print('\n')
		# Update SharePoint Folder
		print('----------------------------------------------------------------------')
		subprocess.run("sh /home/pi/Desktop/DataHistorisationAPP/McView-DataServer/syncIoTGatewayStatus.sh", shell=True)
		print('----------------------------------------------------------------------')
		sleep(1)
		print('\n')
		print("[ McView MSG ] - /IoT Gateway Status: SharePoint Folder Updated")

	except:
		print('\n')
		print("[ ERROR 04 ] - /IoT Gateway Status: Sharepoint Syncro Failure")

	
	# ---------------------    Wave Exit  ---------------------------------------------------------------------------
	print('\n')
	LoopApi = LoopApi + 1
	print('------------------------------')
	print('Data Wave #: ' + str(LoopApi))
	print('Data Wave Started at: ' + str(CycleStartTime))
	CycleFinishTime = datetime.now()
	print('Data Wave Finished at: ' + str(CycleFinishTime))
	CycleDuration_seconds = (CycleFinishTime - CycleStartTime).seconds
	CycleDuration_minutes = int(CycleDuration_seconds/60)
	print('Wave Duration: ' + str(CycleDuration_minutes) + ' min.')
	print('------------------------------')
	print('\n')

	TimerforWave = int(3300 - CycleDuration_seconds)
	print('[ McView MSG ] - Waiting for next Data Wave...')
	print('\n')
	sleep(TimerforWave)



	print('[ McView MSG ] - Data Wave will start in 5 min')
	sleep(120)
	print('[ McView MSG ] - Data Wave will start in 3 min')
	sleep(120)
	print('[ McView MSG ] - Data Wave will start in 1 min')
	sleep(60)
	print('[ McView MSG ] - Data Wave Initialization')


####################################### END Main LOOP #######################################
