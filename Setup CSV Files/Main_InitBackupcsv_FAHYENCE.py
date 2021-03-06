import os
import numpy as np 
import pandas as pd
from datetime import datetime

# Functions

def filter(nID,path,DFgen,conca):
	if conca == 1:
		df_filtro = DFgen.loc[DFgen.TagId == nID,: ]	#Temperatura Ambiente Exterior Compression tt21
		df_histo = pd.read_excel(path)
		df_Concatenate = pd.concat([df_histo, df_filtro], sort=True)
		df_Concatenate.to_excel(path, index=False)

	if conca == 0:
		df_filtro = DFgen.loc[DFgen.TagId == nID,: ]	#Temperatura Ambiente Exterior Compression tt21
		df_filtro.to_excel(path, index=False)


def InitDataFrames(StartupFile,path):
	df_Init = pd.read_excel(StartupFile)
	#df Init.to excel(path, index=False)
	df_Init.to_csv(path ,sep=',', index=False , encoding='utf-8')



StartUpFile =  'StartupFile.xlsx'

# HRS - FAHYENCE McView TAGS
IDsTag = {1:[1,"TT 10.2"],
2:[1,"TT 10.3a"],
3:[1,"PT 01"],
4:[1,"PT 2.1"],
5:[1,"PT 2.2"],
6:[1,"PT 3.1"],
7:[1,"PT 3.2"],
8:[1,"PT 3.3"],
9:[1,"PT 4.2"],
10:[1,"TT 4.1"],
11:[1,"TT 4.3"],
12:[1,"TT 4.2"],
13:[1,"TT 4.4"],
27:[1,"FT 04"],
28:[1,"PCV 04"],
29:[1,"PTol Min"],
40:[1,"PTol Max"],
45:[1,"PT 4.1"],
50:[1,"PT 31a"],
51:[1,"DG 01"],
52:[1,"DG 02"],
53:[1,"TT 31"],
54:[1,"TT 32a"],
55:[1,"TT 32b"],
56:[1,"HSS System"],
59:[1,"DG 03"],
60:[1,"TT 10.1"],
62:[1,"Etat Mclyzer STACK 02"],
63:[1,"TT 3.1"],
64:[1,"PT 10"],
65:[1,"P_Puiss_Mclyzer"],
66:[1,"I_Puissance_Mclyzer"],
67:[1,"U_Puissance_Mclyzer"],
68:[1,"P_Aux_Mclyzer"],
69:[1,"P_HRS"],
70:[1,"I_Puissance_HRS"],
71:[1,"U_Puissance_HRS"],
72:[1,"I_Cooling_Unit"],
73:[1,"PT 31b"],
74:[1,"I_Aux_Mclyzer"],
75:[1,"U_Aux_Mclyzer"],
76:[1,"U_CoollingUnit"],
77:[1,"P_CoolingUnit"],
78:[1,"P_Onduleur"],
79:[1,"V_Onduleur"],
80:[1,"I_Onduleur"],
81:[1,"R 4.1"],
82:[1,"Etat_CMH2"],
83:[1,"Etat_Chiller"],
88:[1,"P0_derniere_recharge"],
89:[1,"Pf_derniere_recharge"],
90:[1,"Duree_Dern_rechargess"],
91:[1,"QteH2_derniere_Recharge"],
92:[1,"CumulNbre_Recharge"],
93:[1,"Nbre_Recharge_jour"],
94:[1,"QteH2MoyPeriode"],
95:[1,"QH2Jour"],
96:[1,"HrChiller"],
97:[1,"HrCompression"],
98:[1,"HrCoolingUnit"],
99:[1,"HrCompresseurAirComprime"],
100:[1,"QteH2Periode"],
101:[1,"TT 4.5"],
103:[1,"TotalisateurH2"],
105:[1,"ResultatEtanch??it??"],
106:[1,"PCV 4.322"],
108:[1,"APRR_2"],
109:[1,"P_Target2"],
113:[1,"QH2_avant_compression"],
117:[1,"D??faut_ELY"],
145:[1,"AcquitIHM"],
147:[1,"AcquitClient"],
150:[1,"kVAh_Puis_ELY"],
151:[1,"KVAh_Commande_ELY"],
152:[1,"KVAh_CoolingUnit"],
153:[1,"KVAh_PuisHRS"],
168:[1,"OVLT_B"],
193:[1,"Temp_Distribution"],
194:[1,"Corridor_min"],
195:[1,"Corridor_max"],
198:[1,"Corridor_min_temp"],
199:[1,"Corridor_max_temp"],
201:[1,"TT 10.3b"],
205:[1,"Delta_Pression_Test_fuite"],
206:[1,"Cpt_CyclePress_1"],
207:[1,"Cpt_CyclePress_2"],
208:[1,"Cpt_CyclePress_3"],
209:[1,"Cpt_T_dist_actu_Hours"],
210:[1,"Cpt_T_dist_actu_HH"],
212:[1,"Cpt_T_dist_preced_Hours"],
214:[1,"VFC31_Valve_Status"],
215:[1,"VFC1_Valve_Status"],
216:[1,"VFC32_Valve_Status"],
217:[1,"VFC33_Valve_Status"],
218:[1,"VFC41_Valve_Status"],
219:[1,"VFC42_Valve_Status"],
220:[1,"VFC2_Valve_Status"],
221:[1,"ChillerR2_CptFctHor_Heure"],
223:[1,"CM2_1_CptFctHor_Heure"],
224:[1,"CM1_0_CptFctHor_Heure"],
233:[1,"Source_pressure_1"],
234:[1,"Source_pressure_2"],
237:[1,"H2_Dist_Temp"],
243:[1,"START_MAINTENANCE"],
244:[1,"STOP_MAINTENANCE"],
245:[1,"RM_CompAC_C10"],
246:[1,"Disj_CompAC_C10"],
247:[1,"RM_CompH2_C2_1"],
248:[1,"Disj_CompH2_C2_1"],
249:[1,"RMEP_OilChiller_R2"],
250:[1,"Disj_OilChiller_R2"],
251:[1,"RM_RefroidH2_R4_1"],
252:[1,"Disj_RefroidH2_R4_1"],
253:[1,"Def_OilChiller_R2"],
256:[1,"Disjonction_Compresseur_Cooling_Unit"],
257:[1,"Pressostat_BP_Groupe_Froid"],
258:[1,"Pressostat_HP_Groupe_Froid"],
260:[1,"Safety_Module_Reset_Plant"],
261:[1,"Air_Compressor_Running"],
263:[1,"H2_compressor_Running"],
264:[1,"Powering_Oil_Cooler_Compressor"],
265:[1,"Running_Oil_Cooler_Compressor"],
266:[1,"Running_H2_Dist_Chiller"],
267:[1,"Stopped_Compressor"],
268:[1,"Stopped_Dist"],
269:[1,"Ack_FIS4_Def"],
270:[1,"H2_Source_Valve"],
277:[1,"Downstream_Compressor_Valve"],
278:[1,"Storage_1_Valve"],
279:[1,"Storage_2_Valve"],
280:[1,"Storage_3_Valve"],
281:[1,"Distribution_Plate_Inlet_Valve"],
282:[1,"Distribution_drain_Valve"],
283:[1,"Distribution_flex_inlet_Valve"],
284:[1,"Distribution_flex_Drain_Valve"],
285:[1,"BP_Start_Dist"],
286:[1,"BP_Stop_Dist"],
287:[1,"Disj_Cooling_Unit_dist"],
288:[1,"Discrepancy_Cooling_Unit_Cmd"],
289:[1,"PLC_Watch_dog_Default"],
290:[1,"TOR_Input_Card_Default"],
291:[1,"TOR_Output_Card_Default"],
292:[1,"ANA_Output_Card_Default"],
293:[1,"Comm_Loss_PLC"],
294:[1,"Comm_Loss_HMI_Dist"],
295:[1,"Comm_Loss_HMI_Maintenance"],
296:[1,"400V_Power_Loss"],
297:[1,"230V_Backup_Power_Loss"],
298:[1,"Compressed_Air_Loss_VCF1_VCF2"],
299:[1,"Discrepancy_cmd_VCF1_VCF2"],
300:[1,"Compressed_Air_Loss_VCF3_1_VCF3_2_VCF3_3"],
301:[1,"Discrepancy_cmd_VCF3_1_VCF3_2_VCF3_3"],
302:[1,"Compressed_Air_Loss_VCF4_1_VCO4_2"],
303:[1,"Discrepancy_cmd_VCF4_1_VCF4_2"],
304:[1,"TSL43_Dist_Temp_Low"],
305:[1,"TT44_Dist_Temp_OoS"],
306:[1,"TSH45_Dist_Temp_High"],
307:[1,"TSL45_Dist_Temp_Low"],
308:[1,"TT42_TT43_Dist_Temp_OoS"],
309:[1,"TT4_Dist_Temp_OoS"],
310:[1,"TSLL45_Dist_Temp_LL"],
311:[1,"TSLL4_Dist_Temp_HH"],
312:[1,"Flexible_Pressure_OoS_BeforeR4"],
313:[1,"PSH41_Flexible_Pressure_H"],
314:[1,"Flexible_Pressure_OoS_AfterR4"],
315:[1,"PSH42_Flexible_Pressure_H"],
316:[1,"PT41_PT42_Dist_pressure_OoS"],
317:[1,"PSHH4_Dist_pressure_HH"],
318:[1,"PT10_Compressed_Air_Oos"],
319:[1,"PT10_Compressed_Air_H"],
320:[1,"PT10_Compressed_Air_L"],
321:[1,"Cooling_Unit_Pressure_L"],
322:[1,"Cooling_Unit_Pressure_H"],
323:[1,"Cooling_Unit_Pressure_Disj"],
324:[1,"DG3_Defaut"],
325:[1,"General_Pocess_Default"],
326:[1,"FIS_1_Default"],
327:[1,"FIS_2_Default"],
328:[1,"FIS_3_Default"],
329:[1,"FIS_4_Default"],
330:[1,"Default_HARD"],
331:[1,"Default_UTILITY"],
332:[1,"DG1_OoS"],
333:[1,"DG1_Defaut"],
334:[1,"DG2_OoS"],
335:[1,"DG2_Defaut"],
336:[1,"C10_Compressed_Aire_Disj"],
337:[1,"C10_Cmd_Discrepency"],
338:[1,"HSS4_local_EmStop"],
339:[1,"HSS2_Dist_EmStop"],
340:[1,"HSS3_Plant_EmStop"],
341:[1,"TT10A_Amb_Temp_OoS"],
342:[1,"TSL10A_Amb_Temp_L"],
343:[1,"TSH10A_Amb_Temp_H"],
344:[1,"TT10B_Amb_Temp_OoS"],
345:[1,"TSL10B_Amb_Temp_L"],
346:[1,"TSH10B_Amb_Temp_H"],
347:[1,"TT10C_Amb_Temp_OoS"],
348:[1,"TSL10C_Amb_Temp_L"],
349:[1,"TSH10C_Amb_Temp_H"],
350:[1,"Fire_Detector_OoS"],
351:[1,"Fire"],
352:[1,"PT1_Source"],
353:[1,"PSL1_Source"],
354:[1,"PSH1_Source"],
355:[1,"PSL21_Input_Compressor"],
356:[1,"PSLL21_Input_Compressor"],
357:[1,"PSHH21_Input_Compressor"],
358:[1,"H2_Compressor_Disj"],
359:[1,"H2_Compressor_Disrepency"],
360:[1,"H2_Compressor_Chiller_Disj"],
361:[1,"H2_Compressor_Chiller_Discrepency"],
362:[1,"PT22_Output_Compressor"],
363:[1,"PSL22_Output_Compressor"],
364:[1,"PSH22_Output_Compressor"],
365:[1,"PSHH22_Output_Compressor"],
366:[1,"PSHH23_Output_Compressor"],
367:[1,"PSHH24_Level1_Compressor"],
368:[1,"PSHH25_Level2_Compressor"],
369:[1,"PSHH26_Oil_Compressor"],
370:[1,"PT31_Storage1"],
371:[1,"PSL31_Storage1"],
372:[1,"PSH31_Storage1"],
373:[1,"PT32_Storage2"],
374:[1,"PSL32_Storage2"],
375:[1,"PSH32_Storage2"],
376:[1,"PT33_Storage3"],
377:[1,"PSL33_Storage3"],
378:[1,"PSH33_Storage3"],
379:[1,"Pressure_HPStorage_OoS"],
380:[1,"TT31_Storage1"],
381:[1,"TSHH31_Storage1"],
382:[1,"R4_Disj"],
383:[1,"R4_Discrepency"],
384:[1,"FT4_Dist"],
385:[1,"FSHH4_Dist"],
386:[1,"TT4.1_Dist_BeforeR4"],
387:[1,"TSH4.1_Dist_BeforeR4"],
388:[1,"TSL4.1_Dist_BeforeR4"],
389:[1,"TT4.2_Dist_AfterR4"],
390:[1,"TSH4.2_Dist_AfterR4"],
391:[1,"TSL4.2_Dist_AfterR4"],
392:[1,"Dist_Temp_OoS"],
393:[1,"TT4.3_Dist"],
394:[1,"TSH4.3_Dist"],
395:[1,"PB_Drain_Pipe"],
396:[1,"PB_Drain_Storage"],
400:[1,"PB_Stop_Dist"],
401:[1,"VFC1_Remote_Cmd_Open"],
402:[1,"VFC31_Remote_Cmd_Open"],
403:[1,"VFC32_Remote_Cmd_Open"],
404:[1,"VFC33_Remote_Cmd_Open"],
405:[1,"VFC41_Remote_Cmd_Open"],
406:[1,"VFC42_Remote_Cmd_Open"],
407:[1,"VFC2_Remote_Cmd_Open"],
417:[1,"Stop_compression"],
420:[1,"Start_degrade_mode"],
421:[1,"Stop_degrade_mode"],
422:[1,"Start_Compression_degrade_mode"],
423:[1,"Stop_Compression_degrade_mode"],
424:[1,"Working_mode_degrade"],
425:[1,"Ack_HMI"],
426:[1,"Rearm_HMI"],
429:[1,"Dist_HMI_Status"],
430:[1,"SKID_HMI_Status"],
433:[1,"Flexible_Connected"],
434:[1,"PSH4_Status"],
435:[1,"PSH10_1_Status"],
436:[1,"PSH10_2_Status"],
437:[1,"PSH10_3_Status"],
438:[1,"ECV10_1_Status"],
439:[1,"ECV10_2_Status"],
440:[1,"ECV10_3_Status"],
441:[1,"Systeme_EmS_Status"],
442:[1,"Skid_EmS_Status"],
443:[1,"Distrib_Box_EmS_Status"],
444:[1,"FIS01_Status"],
445:[1,"Trisafe_EmS_Status"],
446:[1,"FIS02_Status"],
447:[1,"FIS03_Status"],
448:[1,"FIS04_Status"],
449:[1,"LED_PWR_TRISAFE"],
450:[1,"LED_DATA_TRISAFE"],
451:[1,"LED_ERR_TRISAFE"],
452:[1,"LED_CONF_TRISAFE"],
453:[1,"Etat Mclyzer STACK 01"]}






DataToSave = [1,
2,
3,
4,
5,
6,
7,
8,
9,
10,
11,
12,
13,
27,
28,
29,
40,
45,
50,
51,
52,
53,
54,
55,
56,
59,
60,
62,
63,
64,
65,
66,
67,
68,
69,
70,
71,
72,
73,
74,
75,
76,
77,
78,
79,
80,
81,
82,
83,
88,
89,
90,
91,
92,
93,
94,
95,
96,
97,
98,
99,
100,
101,
103,
105,
106,
108,
109,
113,
117,
145,
147,
150,
151,
152,
153,
168,
193,
194,
195,
198,
199,
201,
205,
206,
207,
208,
209,
210,
212,
214,
215,
216,
217,
218,
219,
220,
221,
223,
224,
233,
234,
237,
243,
244,
245,
246,
247,
248,
249,
250,
251,
252,
253,
256,
257,
258,
260,
261,
263,
264,
265,
266,
267,
268,
269,
270,
277,
278,
279,
280,
281,
282,
283,
284,
285,
286,
287,
288,
289,
290,
291,
292,
293,
294,
295,
296,
297,
298,
299,
300,
301,
302,
303,
304,
305,
306,
307,
308,
309,
310,
311,
312,
313,
314,
315,
316,
317,
318,
319,
320,
321,
322,
323,
324,
325,
326,
327,
328,
329,
330,
331,
332,
333,
334,
335,
336,
337,
338,
339,
340,
341,
342,
343,
344,
345,
346,
347,
348,
349,
350,
351,
352,
353,
354,
355,
356,
357,
358,
359,
360,
361,
362,
363,
364,
365,
366,
367,
368,
369,
370,
371,
372,
373,
374,
375,
376,
377,
378,
379,
380,
381,
382,
383,
384,
385,
386,
387,
388,
389,
390,
391,
392,
393,
394,
395,
396,
400,
401,
402,
403,
404,
405,
406,
407,
417,
420,
421,
422,
423,
424,
425,
426,
429,
430,
433,
434,
435,
436,
437,
438,
439,
440,
441,
442,
443,
444,
445,
446,
447,
448,
449,
450,
451,
452,
453]

for Tag in DataToSave:
	TagArray = IDsTag[Tag]
	TagName = TagArray[1]
	print(TagName)

	path = 'HRS-FAHYENCE\\' + str(TagName) + '.csv'
	InitDataFrames(StartUpFile,path)



