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
	#df_Init.to_excel(path, index=False)
	df_Init.to_csv(path ,sep=';', index=False , encoding='utf-8')



StartUpFile =  'C://Users//admin//OneDrive - McPhy Energy//Bureau//McView Server//StartupFile.xlsx'

# HRS - H2M McView TAGS
IDsTag = {1:[1,"IDC_Constructeur_Nbr_Declenchement_SIF","Compteur nombre declenchement SIF par an (Reseet automatique)"],
2:[2,"IDC_Exploitant_Cpt_Energie_Compresseur","Compteur d energie compresseur"],
3:[3,"IDC_Exploitant_Cpt_Energie_groupe_froid",""],
4:[4,"IDC_Exploitant_Cpt_Energie_Instal","compteur d energie d instalation"],
5:[5,"IDC_Exploitant_Indicateur_An_Nb_HeureStat_Arret","Nombre d heure de la station en arret"],
6:[6,"IDC_Exploitant_Indicateur_An_Nb_HeureStat_Nominal","Nombre d heure de la station en fonctionnement normal"],
7:[7,"IDC_Exploitant_Indicateur_An_Nb_HeurStatDegrade_Comp","Nombre d heure de la station en fonctionnement degrade compression"],
8:[8,"IDC_Exploitant_Indicateur_An_Nb_HeurStatDegradeDistri","Nombre d heure de la station en fonctionnement degrade distribution"],
9:[9,"IDC_Exploitant_Indicateur_An_Nb_HeurStation_Standby","Nombre d heure de la station en standby"],
10:[10,"IDC_Exploitant_Indicateur_Jour_Nb_Echec_Distrib_D1","Nombre d echec distribution par jour ligne 1"],
11:[11,"IDC_Exploitant_Indicateur_Jour_Nb_Echec_Distrib_D2","Nombre d echec distribution par jour ligne 2"],
12:[12,"IDC_Exploitant_Indicateur_Jour_NB_recharge_D1","Nombre de recharge par jour ligne 1"],
13:[13,"IDC_Exploitant_Indicateur_Jour_NB_recharge_D2","Nombre de recharge par jour ligne 2"],
14:[14,"IDC_Exploitant_Max_corant_Instal","max courant instatlation"],
15:[15,"IDC_Exploitant_Max_Courant_compresseur","max courant comporesseur"],
16:[16,"IDC_Exploitant_Max_courant_groupe_Froid",""],
17:[17,"IDC_Exploitant_Productivite","Productivite indicateur par an (Reset automatique)"],
18:[18,"IDC_Exploitant_Test_fuite_non_realise","Tests etancheite periodique non realisee"],
19:[19,"IDC_Exploitant_Totaliseur_H2","Totaliseur_H2: Enreistrement les donnes Ã  chaque remplissage"],
20:[129,"IDC_Nbr_Manoeuvre","Compresseur Air - C12.1"],
21:[129,"IDC_Nbr_Manoeuvre","Compresseur H2 - C2"],
22:[129,"IDC_Nbr_Manoeuvre","Ventilation - C21"],
23:[129,"IDC_Nbr_Manoeuvre","Refroidisseur compresseur H2 - R20"],
24:[130,"IDC_Nbre_Heure","compresseur Air - C12.1"],
25:[130,"IDC_Nbre_Heure","compresseur H2 - C2"],
26:[130,"IDC_Nbre_Heure","Ventilation - C21"],
27:[130,"IDC_Nbre_Heure","Refroidisseur compresseur H2 - R20"],
28:[129,"IDC_Nbr_Manoeuvre","Stockage Lp10"],
29:[129,"IDC_Nbr_Manoeuvre","Stockage Mp11"],
30:[129,"IDC_Nbr_Manoeuvre","Stockage Mp12"],
31:[129,"IDC_Nbr_Manoeuvre","Stockage Mp13"],
32:[129,"IDC_Nbr_Manoeuvre","Stockage Mp14"],
33:[129,"IDC_Nbr_Manoeuvre","Stockage Mp15"],
34:[129,"IDC_Nbr_Manoeuvre","Vanne automatique coupure distribution ligne 1"],
35:[129,"IDC_Nbr_Manoeuvre","Vanne de purge distribution ligne 1"],
36:[129,"IDC_Nbr_Manoeuvre","Vanne de purge ligne 1"],
37:[129,"IDC_Nbr_Manoeuvre","Vanne automatique stockage Mp1 vers distribution 1"],
38:[129,"IDC_Nbr_Manoeuvre","Vanne automatique stockage Mp2 vers distribution 1"],
39:[129,"IDC_Nbr_Manoeuvre","Vanne automatique stockage Mp3 vers distribution 1"],
40:[129,"IDC_Nbr_Manoeuvre","Vanne automatique stockage Mp4 vers distribution 1"],
41:[129,"IDC_Nbr_Manoeuvre","Vanne automatique coupure distribution ligne 2"],
42:[129,"IDC_Nbr_Manoeuvre","Vanne de purge distribution ligne 2"],
43:[129,"IDC_Nbr_Manoeuvre","Vanne de purge ligne 2"],
44:[129,"IDC_Nbr_Manoeuvre","Vanne automatique stockage Mp1 vers distribution 2"],
45:[129,"IDC_Nbr_Manoeuvre","Vanne automatique stockage Mp2 vers distribution 2"],
46:[129,"IDC_Nbr_Manoeuvre","Vanne automatique stockage Mp3 vers distribution 2"],
47:[129,"IDC_Nbr_Manoeuvre","Vanne automatique stockage Mp4 vers distribution 2"],
48:[129,"IDC_Nbr_Manoeuvre","Vanne automatique C2 vers stockage Mp1"],
49:[129,"IDC_Nbr_Manoeuvre","Vanne automatique C2 vers stockage Mp2"],
50:[129,"IDC_Nbr_Manoeuvre","Vanne automatique C2 vers stockage Mp3"],
51:[129,"IDC_Nbr_Manoeuvre","Vanne automatique C2 vers stockage Mp4"],
52:[129,"IDC_Nbr_Manoeuvre","Vanne alumentation en azote"],
53:[129,"IDC_Nbr_Manoeuvre","vanne sortie compression ver stockage"],
54:[129,"IDC_Nbr_Manoeuvre","Vanne purge compresseur d air"],
55:[129,"IDC_Nbr_Manoeuvre","Vanne purge compression"],
56:[129,"IDC_Nbr_Manoeuvre","Vanne recirculation compression"],
57:[129,"IDC_Nbr_Manoeuvre","vanne source vers compression"],
58:[130,"IDC_Nbre_Heure","Vanne automatique coupure distribution ligne 1"],
59:[130,"IDC_Nbre_Heure","Vanne de purge distribution ligne 1"],
60:[130,"IDC_Nbre_Heure","Vanne de purge ligne 1"],
61:[130,"IDC_Nbre_Heure","Vanne automatique stockage Mp1 vers distribution 1"],
62:[130,"IDC_Nbre_Heure","Vanne automatique stockage Mp2 vers distribution 1"],
63:[130,"IDC_Nbre_Heure","Vanne automatique stockage Mp3 vers distribution 1"],
64:[130,"IDC_Nbre_Heure","Vanne automatique stockage Mp4 vers distribution 1"],
65:[130,"IDC_Nbre_Heure","Vanne automatique coupure distribution ligne 2"],
66:[130,"IDC_Nbre_Heure","Vanne de purge distribution ligne 2"],
67:[130,"IDC_Nbre_Heure","Vanne de purge ligne 2"],
68:[130,"IDC_Nbre_Heure","Vanne automatique stockage Mp1 vers distribution 2"],
69:[130,"IDC_Nbre_Heure","Vanne automatique stockage Mp2 vers distribution 2"],
70:[130,"IDC_Nbre_Heure","Vanne automatique stockage Mp3 vers distribution 2"],
71:[130,"IDC_Nbre_Heure","Vanne automatique stockage Mp4 vers distribution 2"],
72:[130,"IDC_Nbre_Heure","Vanne automatique C2 vers stockage Mp1"],
73:[130,"IDC_Nbre_Heure","Vanne automatique C2 vers stockage Mp2"],
74:[130,"IDC_Nbre_Heure","Vanne automatique C2 vers stockage Mp3"],
75:[130,"IDC_Nbre_Heure","Vanne automatique C2 vers stockage Mp4"],
76:[130,"IDC_Nbre_Heure","Vanne alumentation en azote"],
77:[130,"IDC_Nbre_Heure","vanne sortie compression ver stockage"],
78:[130,"IDC_Nbre_Heure","Vanne purge compresseur d air"],
79:[130,"IDC_Nbre_Heure","Vanne purge compression"],
80:[130,"IDC_Nbre_Heure","Vanne recirculation compression"],
81:[130,"IDC_Nbre_Heure","vanne source vers compression"],
82:[20,"MES_EANA_STANDARDS_C_AI_TT_Corps_Comp","Temperature traÃ§age electrique"],
83:[21,"MES_EANA_STANDARDS_COM_AI_DF_STxx_1","Detecteur de flamme H2  - Stockage 1 (OPTION)"],
84:[22,"MES_EANA_STANDARDS_COM_AI_DF_STxx_2","Detecteur de flamme H2  - Stockage 2 (OPTION)"],
85:[23,"MES_EANA_STANDARDS_COM_AI_DF_STxx_3","Detecteur de flamme H2  - Stockage 3 (OPTION)"],
86:[24,"MES_EANA_STANDARDS_COM_AI_DG_H2_ATEX_Cont","Detecteur de fuite H2 - zone compression"],
87:[25,"MES_EANA_STANDARDS_COM_AI_DG_H2_ATEX_Vent_Cont","Detecteur de fuite H2 - aspiration zone compression"],
88:[26,"MES_EANA_STANDARDS_COM_AI_DG_H2_PG_CDS","Detecteur de fuite H2 - platine deportee"],
89:[27,"MES_EANA_STANDARDS_D1_AI_Dx_DF","Detecteur de flamme H2  - HDV x"],
90:[28,"MES_EANA_STANDARDS_D1_AI_Dx_DG_H2","Detecteur de fuite H2 - HDV x"],
91:[29,"MES_EANA_STANDARDS_D1_AI_Dx_PT_amont_a","Transmetteur de pression H2 ligne distribution HDV x"],
92:[30,"MES_EANA_STANDARDS_D1_AI_Dx_PT_amont_b","Transmetteur de pression H2 ligne distribution HDV x"],
93:[31,"MES_EANA_STANDARDS_D1_AI_Dx_PT_aval_b","Transmetteur de pression H2 ligne distribution HDV x"],
94:[32,"MES_EANA_STANDARDS_D1_AI_Dx_TT_Aval","Transmetteur de temperature H2 ligne de distribution HDV x"],
95:[33,"MES_EANA_STANDARDS_D2_AI_Dx_DF","Detecteur de flamme H2  - HDV x"],
96:[34,"MES_EANA_STANDARDS_D2_AI_Dx_DG_H2","Detecteur de fuite H2 - HDV x"],
97:[35,"MES_EANA_STANDARDS_D2_AI_Dx_PT_amont_a","Transmetteur de pression H2 ligne distribution HDV x"],
98:[36,"MES_EANA_STANDARDS_D2_AI_Dx_PT_amont_b","Transmetteur de pression H2 ligne distribution HDV x"],
99:[37,"MES_EANA_STANDARDS_D2_AI_Dx_PT_aval_b","Transmetteur de pression H2 ligne distribution HDV x"],
100:[38,"MES_EANA_STANDARDS_D2_AI_Dx_TT_Aval","Transmetteur de temperature H2 ligne de distribution HDV x"],
101:[39,"MES_SANA_STANDARDS_D1_AO_Dx_PCV","Detendeur pilote - HDV x"],
102:[40,"MES_SANA_STANDARDS_D2_AO_Dx_PCV","Detendeur pilote - HDV x"],
103:[41,"MES_EANA_SECU_C_AIF_PT_In_1st_head_a","Pression aspiration compresseur C2-1"],
104:[42,"MES_EANA_SECU_C_AIF_PT_In_1st_head_b","Pression aspiration compresseur C2-2"],
105:[43,"MES_EANA_SECU_C_AIF_PT_Out_1st_head_a","Transmetteur de pression InterEtage compresseur C2- 1"],
106:[44,"MES_EANA_SECU_C_AIF_PT_Out_1st_head_b","Transmetteur de pression InterEtage compresseur C2- 2"],
107:[45,"MES_EANA_SECU_C_AIF_PT_Out_2nd_head_a","Transmetteur de pression aval compresseur C2- 1"],
108:[46,"MES_EANA_SECU_C_AIF_PT_Out_2nd_head_b","Transmetteur de pression aval compresseur C2- 2"],
109:[47,"MES_EANA_SECU_C_AIF_TT_Out_1st_Head_a","Transmetteur de temperature inter-etage compresseur 1"],
110:[48,"MES_EANA_SECU_C_AIF_TT_Out_1st_Head_b","Transmetteur de temperature aval compresseur 1"],
111:[49,"MES_EANA_SECU_C_AIF_TT_Out_2nd_Head_a","Transmetteur de temperature inter-etage compresseur 2"],
112:[50,"MES_EANA_SECU_C_AIF_TT_Out_2nd_Head_b","Transmetteur de temperature aval compresseur 2"],
113:[51,"MES_EANA_SECU_COM_AIF_DG_O2_ATEX_Cont","Detecteur de manque O2  - zone compression"],
114:[52,"MES_EANA_SECU_COM_AIF_PT_air_comprime_a","Transmetteur de pression ballon d air comprime"],
115:[53,"MES_EANA_SECU_COM_AIF_PT_Azote_a","Transmetteur de pression Alim Azote"],
116:[54,"MES_EANA_SECU_COM_AIF_PT_Azote_b","Transmetteur de pression Alim Azote"],
117:[55,"MES_EANA_SECU_COM_AIF_PT_ST10_a","Transmetteur de pression Source -1"],
118:[56,"MES_EANA_SECU_COM_AIF_PT_ST10_b","Transmetteur de pression Source -2"],
119:[57,"MES_EANA_SECU_COM_AIF_PT_ST11_a","Transmetteur de pression Stockage MP 1"],
120:[58,"MES_EANA_SECU_COM_AIF_PT_ST11_b","Transmetteur de pression Stockage MP 1"],
121:[59,"MES_EANA_SECU_COM_AIF_PT_ST12_a","Transmetteur de pression Stockage MP 2"],
122:[60,"MES_EANA_SECU_COM_AIF_PT_ST12_b","Transmetteur de pression Stockage MP 2"],
123:[61,"MES_EANA_SECU_COM_AIF_PT_ST13_a","Transmetteur de pression Stockage MP 3"],
124:[62,"MES_EANA_SECU_COM_AIF_PT_ST13_b","Transmetteur de pression Stockage MP 3"],
125:[63,"MES_EANA_SECU_COM_AIF_PT_ST14_a","Transmetteur de pression Stockage MP 4"],
126:[64,"MES_EANA_SECU_COM_AIF_PT_ST14_b","Transmetteur de pression Stockage MP 4"],
127:[65,"MES_EANA_SECU_COM_AIF_TT_Amb_ATEX_Cont","Temperature Ambiance exterieure  compression"],
128:[66,"MES_EANA_SECU_COM_AIF_TT_amb_ext_container","Temperature Ambiance exterieure du conteneur"],
129:[67,"MES_EANA_SECU_COM_AIF_TT_amb_non_ATEX_Cont","Temperature Ambiance exterieure zone utilites"],
130:[68,"MES_EANA_SECU_COM_AIF_TT_amb_platine_dist","Temperature Ambiance Platine deporte"],
131:[69,"MES_EANA_SECU_COM_AIF_TT_amb_stockages","Temperature Ambiance zone de stockage"],
132:[70,"MES_EANA_SECU_D1_AIF_Dx_FT_a","Transmetteur de debit H2 ligne distribution HDV x"],
133:[71,"MES_EANA_SECU_D1_AIF_Dx_PT_Aval_Secu_a","Transmetteur de pression H2 ligne distribution HDV x"],
134:[72,"MES_EANA_SECU_D1_AIF_Dx_PT_Aval_Secu_b","Transmetteur de pression H2 ligne distribution HDV x"],
135:[73,"MES_EANA_SECU_D1_AIF_Dx_TT_amb_dispenser","Temperature Ambiance Dispenser HDV x"],
136:[74,"MES_EANA_SECU_D1_AIF_Dx_TT_secu_Aval_a","Transmetteur de temperature H2 ligne de distribution HDV x"],
137:[75,"MES_EANA_SECU_D2_AIF_Dx_FT_a","Transmetteur de debit H2 ligne distribution HDV x"],
138:[76,"MES_EANA_SECU_D2_AIF_Dx_PT_Aval_Secu_a","Transmetteur de pression H2 ligne distribution HDV x"],
139:[77,"MES_EANA_SECU_D2_AIF_Dx_PT_Aval_Secu_b","Transmetteur de pression H2 ligne distribution HDV x"],
140:[78,"MES_EANA_SECU_D2_AIF_Dx_TT_amb_dispenser","Temperature Ambiance Dispenser HDV x"],
141:[79,"MES_EANA_SECU_D2_AIF_Dx_TT_secu_Aval_a","Transmetteur de temperature H2 ligne de distribution HDV x"],
142:[80,"MES_EANA_DEBIT_FT8b_D1_AdcTorBarMeanTemp","Temperature barre de torsion (Â°C)"],
143:[81,"MES_EANA_DEBIT_FT8b_D1_AdcTubeMeanTemp","Temperature Tank (Â°C)"],
144:[82,"MES_EANA_DEBIT_FT8b_D1_MassFlowRate","Debit Masse (g/s)"],
145:[83,"MES_EANA_DEBIT_FT8b_D1_TotalMassFwd","Totalisation Masse comptee (grammes)"],
146:[84,"MES_EANA_DEBIT_FT8b_D1_TotMassRev","Totalisation Masse decomptee (grammes)"],
147:[85,"MES_EANA_DEBIT_FT8b_D2_AdcTorBarMeanTemp","Temperature barre de torsion (Â°C)"],
148:[86,"MES_EANA_DEBIT_FT8b_D2_AdcTubeMeanTemp","Temperature Tank (Â°C)"],
149:[87,"MES_EANA_DEBIT_FT8b_D2_TotalMassFwd","Totalisation Masse comptee (grammes)"],
150:[88,"MES_EANA_DEBIT_FT8b_D2_TotMassRev","Totalisation Masse decomptee (grammes)"],
151:[89,"MES_EANA_DEBIT_FT8b_D2_VolumetricFlowRate","Debit_Volume (l/s)"],
152:[131,"Defaut","SIF-941.S02 (true=OK)"],
153:[131,"Defaut","SIF-943.S02 (true=OK)"],
154:[131,"Defaut","SIF-95.S02 (true=OK)"],
155:[131,"Defaut","Pilotage ESV 12.9 (true=OK)"],
156:[131,"Defaut","Pilotage ESV 12.10 (true=OK)"],
157:[131,"Defaut","TSHL 20.1 (true=OK)"],
158:[131,"Defaut","FSLL 20.1 (true=OK)"],
159:[131,"Defaut","SIF-12.S01 (true=OK)"],
160:[131,"Defaut","SIF-93.S01.03 (true=OK)"],
161:[131,"Defaut","Detecteur de fuite H2 - HDV 1 DGHH 41 (true=OK)"],
162:[131,"Defaut","Detecteur de fuite H2 - HDV 2 DGHH 43 (true=OK)"],
163:[131,"Defaut","Detecteur de fuite H2 - zone compression DGHH 21 (true=OK)"],
164:[131,"Defaut","Detecteur de fuite H2 - aspiration zone compression DGHH 22 (true=OK)"],
165:[131,"Defaut","Arret d urgence station SIF-12 (true=OK)"],
166:[131,"Defaut","ETAT - Pressostat Fuites Compresseur C2- Tete 1"],
167:[131,"Defaut","ETAT - Pressostat Fuites Compresseur C2- Tete 2"],
168:[131,"Defaut","ETAT - Retour de Mise en Puissance Compresseur"],
169:[131,"Defaut","ETAT - Retour de Mise en Puissance du refroidisseur"],
170:[131,"Defaut","ETAT - Pressostat air comprime vers stockage"],
171:[131,"Defaut","ETAT - Retour de Mise en Puissance Compr_Air"],
172:[131,"Defaut","ETAT - Pressostat air comprime vers HDV x"],
173:[131,"Defaut","ETAT - Pressostat air comprime vers HDV x"],
174:[131,"Defaut","ETAT - Defaut electrique du refroidisseur"],
175:[131,"Defaut","ETAT - Disjonction Compresseur"],
176:[131,"Defaut","ETAT - Disjonction principal du refroidisseur"],
177:[131,"Defaut","ETAT - Demarreur Compresseur en defaut"],
178:[131,"Defaut","ETAT - Defaut du debit du refroidisseur"],
179:[131,"Defaut","ETAT - Defaut de temperature du refroidisseur"],
180:[131,"Defaut","ETAT - Disjonction Compr_Air_C12"],
181:[90,"ETATS_ETOR_STANDARDS_D1_DI_Dx_HS_AUTORISATION","ETAT - Autorisation Distribution HDV x"],
182:[131,"Defaut","SIF_02_S01 (true=OK)"],
183:[131,"Defaut","SIF_12_S01 (true=OK)"],
184:[131,"Defaut","SIF_13_S01 (true=OK)"],
185:[131,"Defaut","SIF_14_S01 (true=OK)"],
186:[131,"Defaut","SIF_31_S01 (true=OK)"],
187:[131,"Defaut","SIF_41_S01 (true=OK)"],
188:[131,"Defaut","SIF_43_S01 (true=OK)"],
189:[131,"Defaut","Detections de flamme Dispenser 1 (true=OK)"],
190:[131,"Defaut","Detections de flamme Dispenser 2 (true=OK)"],
191:[131,"Defaut","AU Armoire local electrique (true=OK)"],
192:[131,"Defaut","AU Etage zone refroidisseurs (true=OK)"],
193:[131,"Defaut","AU Platine Gaz compression (true=OK)"],
194:[131,"Defaut","AU Dispenser 1 (true=OK)"],
195:[131,"Defaut","AU Dispenser 2 (true=OK)"],
196:[131,"Defaut","AU Platine deportee (true=OK)"],
197:[131,"Defaut","SIF-11.S01 (true=OK)"],
198:[131,"Defaut","SIF-12.S01.10 (true=OK)"],
199:[131,"Defaut","SIF-12.S01.15 (true=OK)"],
200:[131,"Defaut","SIF-12.S01.20 (true=OK)"],
201:[131,"Defaut","SIF-12.S01.25 (true=OK)"],
202:[131,"Defaut","SIF_13_S01 discordance de feedback (true=defaut)"],
203:[131,"Defaut","SIF_13_S01 discordance de feedback (true=defaut)"],
204:[131,"Defaut","SIF-93.S02 (true=OK)"],
205:[131,"Defaut","SIF-94x/95.S02 (true=OK)"],
206:[131,"Defaut","Temperature ambiante zone Utilites (true=OK)"],
207:[131,"Defaut","Temperature ambiante externe conteneur (true=OK)"],
208:[131,"Defaut","Temperature ambiante zone compression (true=OK)"],
209:[131,"Defaut","Temperature ambiante Dispenser 1 (true=OK)"],
210:[131,"Defaut","Temperature ambiante Dispenser 2 (true=OK)"],
211:[131,"Defaut","Temperature Ambiance Platine deporte (true=OK)"],
212:[131,"Defaut","Temperature Ambiance zone de stockage (true=OK)"],
213:[131,"Defaut","Detection defaut tension d alimentation (true=OK)"],
214:[131,"Defaut","Detection defaut tension reseau automate (true=OK)"],
215:[131,"Defaut","PSHH1.1 : Detection pression H2 alimentation station (true=OK)"],
216:[131,"Defaut","HH Pression alimentation air comprime (true=OK)"],
217:[131,"Defaut","PSHH3.1 : Detection pression stockage MP1 (true=OK)"],
218:[131,"Defaut","PSHH3.2 : Detection pression stockage MP2 (true=OK)"],
219:[131,"Defaut","PSHH3.3 : Detection pression stockage MP3 (true=OK)"],
220:[131,"Defaut","PSHH3.4 : Detection pression stockage MP4 (true=OK)"],
221:[131,"Defaut","LL Pression alimentation air comprime (true=OK)"],
222:[131,"Defaut","Rearmement general SIF (true=rearmer)"],
223:[131,"Defaut","SIF-02.B01.02 (true=OK)"],
224:[131,"Defaut","SIF-02.S01.02 (true=OK)"],
225:[131,"Defaut","SIF-02.S01.10 (true=OK)"],
226:[131,"Defaut","SIF-02.S01.12 (true=OK)"],
227:[131,"Defaut","SIF-02.S01.16 (true=OK)"],
228:[131,"Defaut","SIF-02.S01 Qbad fault (true=defaut)"],
229:[131,"Defaut","TSH 121 Temperature ambiante zone Utilites (true=OK)"],
230:[131,"Defaut","TSH 122 Temperature ambiante externe conteneur (true=OK)"],
231:[131,"Defaut","TSHH 21 Temperature ambiante zone compression (true=OK)"],
232:[131,"Defaut","DG23LL Detection manque oxygene zone atex compresseur (true=OK)"],
233:[131,"Defaut","PSHH 2.1 pession alimentation compresseur C2-1 (true=OK)"],
234:[131,"Defaut","PSHH 2.10 Detection fuites H2 - membre Tete 1 compresseur H2  (true=OK)"],
235:[131,"Defaut","PSHH 2.11 Detection fuites H2 - membre Tete 2 compresseur H2  (true=OK)"],
236:[131,"Defaut","PSHH2.2 pression InterEtage compresseur C2- 1 (true=OK)"],
237:[131,"Defaut","PSHH2.3 pression aval compresseur C2-1 (true=OK)"],
238:[131,"Defaut","PSLL 2.1 pession alimentation compresseur C2-1 (true=OK)"],
239:[131,"Defaut","PSLL 2.12 Detection niveau bas huile compresseur (true=OK)"],
240:[131,"Defaut","PSLL 2.2 pression InterEtage compresseur C2- 1 (true=OK)"],
241:[131,"Defaut","PSLL 2.3 pression aval compresseur C2-1 - voie a (true=OK)"],
242:[131,"Defaut","SIF-31.S01.01 (true=OK)"],
243:[131,"Defaut","SIF-31.S01.02 (true=OK)"],
244:[131,"Defaut","SIF-31.S01.02.03 (true=OK)"],
245:[131,"Defaut","SIF-31.S01.06 (true=OK)"],
246:[131,"Defaut","SIF-31.S01.12 (true=OK)"],
247:[131,"Defaut","SIF-31.S01.19 (true=OK)"],
248:[131,"Defaut","SIF-31.S01.64 (true=OK)"],
249:[131,"Defaut","TSHH 2.1 Temperature inter-etage compresseur 1 (true=OK)"],
250:[131,"Defaut","TSHH 2.2 Temperature inter-etage compresseur 2 (true=OK)"],
251:[131,"Defaut","TSHH 2.3 Temperature aval compresseur 1 (true=OK)"],
252:[131,"Defaut","TSHH 2.4 Temperature aval compresseur 2 (true=OK)"],
253:[131,"Defaut","FSHH 4.101 : debit H2 ligne distribution HDV 1 (true=OK)"],
254:[131,"Defaut","PSHH 4.102 : pression H2 ligne distribution HDV 1 (true=OK)"],
255:[131,"Defaut","PSL 12.3 : Pressostat air comprime (true=OK)"],
256:[131,"Defaut","SIF-41.S01.01 (true=OK)"],
257:[131,"Defaut","SIF-41.S01.02 (true=OK)"],
258:[131,"Defaut","SIF-41.S01.02.03 (true=OK)"],
259:[131,"Defaut","SIF-41.S01.03 (true=OK)"],
260:[131,"Defaut","SIF-41.S01.06 (true=OK)"],
261:[131,"Defaut","SIF-41.S01.09 (true=OK)"],
262:[131,"Defaut","SIF-41.S01.12 (true=OK)"],
263:[131,"Defaut","SIF-41.S01 Qbad fault (true=defaut)"],
264:[131,"Defaut","SIF-41.S02 (true=OK)"],
265:[131,"Defaut","TSHH 4.101 : temperature H2 ligne de distribution HDV 1 (true=OK)"],
266:[131,"Defaut","XSH 41.1 (true=OK)"],
267:[131,"Defaut","XHS 41.2 (true=OK)"],
268:[131,"Defaut","XSH 4.11 Detection presence flexible dispenser 1 (true=OK)"],
269:[131,"Defaut","FSHH 4.301 : debit H2 ligne distribution HDV 2 (true=OK)"],
270:[131,"Defaut","PSHH 4.302 : pression H2 ligne distribution HDV 2 (true=OK)"],
271:[131,"Defaut","PSL 12.4 : Pressostat air comprime (true=OK)"],
272:[131,"Defaut","SIF-43.S01.01 (true=OK)"],
273:[131,"Defaut","SIF-43.S01.02 (true=OK)"],
274:[131,"Defaut","SIF-43.S01.02.03 (true=OK)"],
275:[131,"Defaut","SIF-43.S01.03 (true=OK)"],
276:[131,"Defaut","SIF-43.S01.06 (true=OK)"],
277:[131,"Defaut","SIF-43.S01.09 (true=OK)"],
278:[131,"Defaut","SIF-43.S01.12 (true=OK)"],
279:[131,"Defaut","SIF-43.S01 Qbad fault (true=defaut)"],
280:[131,"Defaut","SIF-43.S02 (true=OK)"],
281:[131,"Defaut","TSHH 4.301 (true=OK)"],
282:[91,"ETATS_STOR_ILOT_COM_DO_D1_ECV_Purge_STK","Vanne automatique Purge ligne HDV1"],
283:[92,"ETATS_STOR_ILOT_COM_DO_D1_ECV_ST11","Vanne automatique Stockage MP1 vers distribution HDV1"],
284:[93,"ETATS_STOR_ILOT_COM_DO_D1_ECV_ST12","Vanne automatique Stockage MP2 vers distribution HDV1"],
285:[94,"ETATS_STOR_ILOT_COM_DO_D1_ECV_ST13","Vanne automatique Stockage MP3 vers distribution HDV1"],
286:[95,"ETATS_STOR_ILOT_COM_DO_D1_ECV_ST14","Vanne automatique Stockage MP4 vers distribution HDV1"],
287:[96,"ETATS_STOR_ILOT_COM_DO_D2_ECV_Purge_STK","Vanne automatique Purge ligne HDV2"],
288:[97,"ETATS_STOR_ILOT_COM_DO_D2_ECV_ST11","Vanne automatique Stockage MP1 vers distribution HDV2"],
289:[98,"ETATS_STOR_ILOT_COM_DO_D2_ECV_ST12","Vanne automatique Stockage MP2 vers distribution HDV2"],
290:[99,"ETATS_STOR_ILOT_COM_DO_D2_ECV_ST13","Vanne automatique Stockage MP3 vers distribution HDV2"],
291:[100,"ETATS_STOR_ILOT_COM_DO_D2_ECV_ST14","Vanne automatique Stockage MP4 vers distribution HDV2"],
292:[101,"ETATS_STOR_ILOT_COM_DO_ECV_C_ST11","Vanne automatique C2 vers Stockage MP1"],
293:[102,"ETATS_STOR_ILOT_COM_DO_ECV_C_ST12","Vanne automatique C2 vers Stockage MP2"],
294:[103,"ETATS_STOR_ILOT_COM_DO_ECV_C_ST13","Vanne automatique C2 vers Stockage MP3"],
295:[104,"ETATS_STOR_ILOT_COM_DO_ECV_C_ST14","Vanne automatique C2 vers Stockage MP4"],
296:[105,"ETATS_STOR_ILOT_D1_DO_Dx_ECV_isolement","Vanne automatique coupure distribution HDV1"],
297:[106,"ETATS_STOR_ILOT_D1_DO_Dx_ECV_Purge_Av","Vanne automatique Purge distribution HDV1"],
298:[107,"ETATS_STOR_ILOT_D2_DO_Dx_ECV_isolement","Vanne automatique coupure distribution HDV1"],
299:[108,"ETATS_STOR_ILOT_D2_DO_Dx_ECV_Purge_Av","Vanne automatique Purge distribution HDV1"],
300:[109,"ETATS_STOR_SECU_COM_DOF_D1_ECV_Alim_1","Alimentation vannes HDV 1-1"],
301:[110,"ETATS_STOR_SECU_COM_DOF_D1_ECV_Alim_2","Alimentation vannes HDV 1-2"],
302:[111,"ETATS_STOR_SECU_COM_DOF_D2_ECV_Alim_1","Alimentation vannes HDV 2-1"],
303:[112,"ETATS_STOR_SECU_COM_DOF_D2_ECV_Alim_2","Alimentation vannes HDV 2-2"],
304:[113,"ETATS_STOR_SECU_COM_DOF_ECV_Circuit_Alim_Gen","Alimentation generale air platine depuis le local electrique "],
305:[114,"ETATS_STOR_SECU_COM_DOF_ECV_Circuit_Comp_H2","Mise en securite air comprime -Compression"],
306:[115,"ETATS_STOR_SECU_COM_DOF_ECV_Platine_STxx","Alimentation vannes stockage"],
307:[116,"ETATS_STOR_SECU_COM_DOF_ECV_ST11","Alimentation vannes isolement Stock MP 1"],
308:[117,"ETATS_STOR_SECU_COM_DOF_ECV_ST12","Alimentation vannes isolement Stock MP 2"],
309:[118,"ETATS_STOR_SECU_COM_DOF_ECV_ST13","Alimentation vannes isolement Stock MP 3"],
310:[119,"ETATS_STOR_SECU_COM_DOF_ECV_ST14","Alimentation vannes isolement Stock MP 4"],
311:[120,"ETATS_STOR_SECU_COM_DOF_Purge_Comp_Air","Purge generale air comprime"],
312:[121,"ETATS_STOR_STANDARDS_C_DO_ECV_Out_2nd_Head","Pilotage ECV2.3 sortie compression vers stockage"],
313:[122,"ETATS_STOR_STANDARDS_C_DO_ECV_Purge_Comp_H2","Pilotage ECV2.5 purge comression"],
314:[123,"ETATS_STOR_STANDARDS_C_DO_ECV_Rec_Glob_Comp_H2","Pilotage ECV2.4 recirculation compression"],
315:[124,"ETATS_STOR_STANDARDS_C_DO_ECV_S_STxx","Pilotage ECV1.1 source vers comp"],
316:[125,"ETATS_STOR_STANDARDS_COM_DO_ECV_Nitrogen","Pilotage ECV30.1 alimentation en azote"],
317:[126,"ETATS_STOR_STANDARDS_COM_DO_Marche_Extr_Air","Marche ventilation C21"],
318:[131,"Defaut","Rearmement Fuites sur stockage MP1 Memorized (true=OK)"],
319:[131,"Defaut","Rearmement Fuites sur stockage MP2 Memorized (true=OK)"],
320:[131,"Defaut","Rearmement Fuites sur stockage MP3 Memorized (true=OK)"],
321:[131,"Defaut","Rearmement Fuites sur stockage MP4 Memorized (true=OK)"],
322:[131,"Defaut","Rearmement Fuites generales sur les reseaux gaz (test journalier)"],
323:[131,"Defaut","Rearmement Memorized SIF general   "],
324:[131,"Defaut","SIF-02.B01.10 Memorized (true=OK)"],
325:[131,"Defaut","SIF-31.B01.01 Memorized (true=OK)"],
326:[131,"Defaut","SIF-31.S02 Memorized (true=OK)"],
327:[131,"Defaut","SIF-41.B01.02 Memorized (true=OK)"],
328:[131,"Defaut","SIF-41.B01.07 Memorized (true=OK)"],
329:[131,"Defaut","SIF-41.B01.08 Memorized (true=OK)"],
330:[131,"Defaut","SIF-41.S02 Memorized (true=OK)"],
331:[131,"Defaut","SIF-43.B01.02 Memorized (true=OK)"],
332:[131,"Defaut","SIF-43.B01.07 Memorized (true=OK)"],
333:[131,"Defaut","SIF-43.B01.08 Memorized (true=OK)"],
334:[199,"SPARE",""],
335:[199,"SPARE",""],
336:[199,"SPARE",""],
337:[199,"SPARE",""],
338:[199,"SPARE",""],
339:[199,"SPARE",""],
340:[199,"SPARE",""],
341:[199,"SPARE",""],
342:[199,"SPARE",""],
343:[199,"SPARE",""],
344:[199,"SPARE","MÃ©morisation dÃ©faut groupe materiel (1=memorise)"],
345:[199,"SPARE","MÃ©morisation dÃ©faut groupe alarme stockage (1=memorise)"],
346:[199,"SPARE","Memorisation defaut carte entree sortie (1=memorise)"],
347:[199,"SPARE","Memorisation defaut groupe communication (1=memorise)"],
348:[199,"SPARE","Memorisation defaut groupe chien de garde (1=memorise)"],
349:[199,"SPARE","Memorisation defaut phoenix controller (1=memorise)"],
350:[199,"SPARE","Memorisation defaut groupe SIF (1=memorise)"],
351:[199,"SPARE","Memorisation defaut groupe stockage (1=memorise)"],
352:[199,"SPARE","Memorisation envoi du SMS cylique (1=memorise)"],
353:[127,"IDC_D1_Productivite_Dx","Productivite journaliere (en m3)"],
354:[128,"IDC_D2_Productivite_Dx","Productivite journaliere (en m3)"],
355:[199,"SPARE","Envoi mail historique par ViewON"],
356:[199,"SPARE","Recharge distribution en cours D1 ou d2 (true=en cours)"],
357:[199,"SPARE","Etat station (true=OK)"],
358:[199,"SPARE","Memorisation defaut groupe grafcet (1=emorise)"]}






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
14,
15,
16,
17,
18,
19,
20,
21,
22,
23,
24,
25,
26,
27,
28,
29,
30,
31,
32,
33,
34,
35,
36,
37,
38,
39,
40,
41,
42,
43,
44,
45,
46,
47,
48,
49,
50,
51,
52,
53,
54,
55,
56,
57,
58,
59,
60,
61,
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
84,
85,
86,
87,
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
102,
103,
104,
105,
106,
107,
108,
109,
110,
111,
112,
113,
114,
115,
116,
117,
118,
119,
120,
121,
122,
123,
124,
125,
126,
127,
128,
129,
130,
131,
132,
133,
134,
135,
136,
137,
138,
139,
140,
141,
142,
143,
144,
145,
146,
147,
148,
149,
150,
151,
152,
153,
154,
155,
156,
157,
158,
159,
160,
161,
162,
163,
164,
165,
166,
167,
168,
169,
170,
171,
172,
173,
174,
175,
176,
177,
178,
179,
180,
181,
182,
183,
184,
185,
186,
187,
188,
189,
190,
191,
192,
193,
194,
195,
196,
197,
198,
199,
200,
201,
202,
203,
204,
205,
206,
207,
208,
209,
210,
211,
212,
213,
214,
215,
216,
217,
218,
219,
220,
221,
222,
223,
224,
225,
226,
227,
228,
229,
230,
231,
232,
233,
234,
235,
236,
237,
238,
239,
240,
241,
242,
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
254,
255,
256,
257,
258,
259,
260,
261,
262,
263,
264,
265,
266,
267,
268,
269,
270,
271,
272,
273,
274,
275,
276,
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
358]

for Tag in DataToSave:
	TagArray = IDsTag[Tag]
	TagName = TagArray[1]
	print(TagName)

	path = 'X:\\McView Historisation\\HRS-Bethune\\' + str(TagName) + '.csv'
	InitDataFrames(StartUpFile,path)


