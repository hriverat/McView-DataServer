# coding=utf-8

##### Chargement packages
from statistics import mean
import numpy as np
import pandas as pd
import csv
import statistics
import datetime as dt
from datetime import datetime, timedelta
import configparser
import os


def CleanData(path_FWD, path_PT, path_PTD, path_Pinit, path_RefgTyp, path_ComTyp, OutData, projet, no_dispenser, dispenser_type, dispenser_pressure):
    """Fonction permettant de resumé les information de
        ravitaillement des stations

    Args:
        path_FWD (path): lien vers le fichier csv Mass Fwd
        path_PT (path): lien vers le fichier PT
        path_PTD (path): lien vers le fichier PTarget
        path_Pinit (path): lien vers le fichier Pinit
        path_RefgTyp (path): lien vers le fichier Refueling end type
        path_ComTyp (path): lien vers le fichier Communication type
        OutData (Char): nom du fichier csv de sortie en vu de le réutiliser 
        projet (Char): nom de la station
        no_dispenser (int): numero de la station
        dispenser_type (Char): type de la station
        dispenser_pressure (Char): Pression
    """
    
    ##### Lecture des fichiers csv
    data1 = pd.read_csv(path_FWD)
    data2 = pd.read_csv(path_PT)
    data3 = pd.read_csv(path_PTD)
    data4 = pd.read_csv(path_Pinit)
    data5 = pd.read_csv(path_RefgTyp)
    data6 = pd.read_csv(path_ComTyp)
    
    
    """Dans cette section, on supprime les colonnes unitiles
        et on procède la suppression des lignes successivement redondants
    """ 
    ### Suppression de colonnes unitiles   
    data1.drop(data1.iloc[:,2:7],1,inplace=True) 
    #print(data1)
    #print(type(data1))

    ### Suppression des lignes successivement redondants
    idx1 = []
    #val = []
    for i in range(len(data1)-1):
        if data1['Value'][i] == data1['Value'][i+1]:
            #val.append(data1['Value'][i+1])
            idx1.append(data1.index[i+1])
        
    #print(idx1)
    #print(val)

    data1.drop(idx1 , inplace=True)
    data1.reset_index(drop=True, inplace=True)
    #print(data1)
    ######################################################################
    
    
    
    ##### Variable par defaut de la pression de chaque station
    pression_defaut = 0
    if projet == "HRS-IP1 D1" and "HRS-IP1 D2" and "HRS-H2M" and "HRS1-SMTAG D1" and "HRS1-SMTAG D2":
        pression_defaut = 390
    if projet == "HRS-H2M":
        pression_defaut = 786.9999694824219
    
    #######################################################################################
    """Dans cette section on recupère les deux colonnes d'interêt (Value et TmeStr)
        En suite on recupère dans les deux boucles la date1/quantité de debut de chargement
            et la date2/quantité de fin de chargement
    """
    ###### Date 1 et quantité de debut #####
    datePrec = []
    quantitePrec = []

    # Boucle qui recupère la date 1 et la quantité à la date 1
    for i in data1.index: 
        if data1['Value'][i] == 0:
            datePrec.append(data1['TimeStr'][i])
            quantitePrec.append(data1['Value'][i])

    #print(datePrec)

    # Concatenation des deux vecteurs sur une seule ligne  
    #Date_quantite1 = np.concatenate((datePrec, quantitePrec))
    # Redimensionnement de vecteur de une ligne en une matrice (n*p)
    #Date_quantite1 = np.reshape(Date_quantite1, (ligne,2), order='F')

    Date_quantite1 = pd.DataFrame()
    Date_quantite1['Date_debut'] = datePrec
    Date_quantite1['Quantity'] = quantitePrec
    Date_quantite1["Date_debut"] = pd.to_datetime(Date_quantite1.Date_debut)
    #print(Date_quantite1)


    ###### Date 2 et quantité de fin #####
    # On utilise le même principe que pour date 1 et quantité de debut

    # supprimer la première ligne qui est égale à 0
    data1.drop(0,0,inplace=True)


    quantiteSuiv = []
    dateSuiv = []

    for i in data1.index:
        if data1['Value'][i] == 0:
            dateSuiv.append(data1['TimeStr'][i-1])
            quantiteSuiv.append(data1['Value'][i-1]/1000)
        
    n=len(data1)
    dateSuiv.append(data1['TimeStr'][n])
    quantiteSuiv.append(data1['Value'][n]/1000)

    # print(dateSuiv)
    # print(quantiteSuiv)


    Date_quantite2 = pd.DataFrame()
    Date_quantite2['Date_fin'] = dateSuiv
    Date_quantite2['Quantity'] = quantiteSuiv
    Date_quantite2["Date_fin"] = pd.to_datetime(Date_quantite2.Date_fin)
    #print(Date_quantite2)
    ligne = len(dateSuiv)
    col = len(quantiteSuiv)
    ################################################################### 
    
    
    """Construction des différentes colonnes,
        conversion en dataframe,
            et concatenation
    """

    # Construction temps de ravitaillement (Unité : mininute)
    Date_quantite2['diff'] = (Date_quantite2["Date_fin"] - Date_quantite1["Date_debut"]).astype('timedelta64[m]')
    timeF = Date_quantite2['diff'].to_numpy()
    a = Date_quantite1["Date_debut"].squeeze().reset_index(drop=True)
    b = Date_quantite2["Date_fin"].squeeze().reset_index(drop=True)


    ###### Construction des colonnes projet, No_dispenser, Dispenser_Type et Dispenser_Pressure
    Projet = []
    No_Dispenser = []
    Dispenser_Type = []
    Dispenser_Pressure = []
    for x in range(len(Date_quantite1['Date_debut'])):
        Projet.append(projet)
        No_Dispenser.append(no_dispenser)
        Dispenser_Type.append(dispenser_type)
        Dispenser_Pressure.append(dispenser_pressure)
        


    ###### conversion en dataframe
    Projet = pd.DataFrame(Projet)
    No_Dispenser = pd.DataFrame(No_Dispenser)
    Dispenser_Type = pd.DataFrame(Dispenser_Type)
    Dispenser_Pressure = pd.DataFrame(Dispenser_Pressure)
    date1 = pd.DataFrame(Date_quantite1['Date_debut'])
    date2 = pd.DataFrame(Date_quantite2['Date_fin'])
    quantity = pd.DataFrame(Date_quantite2['Quantity'])
    timeF = pd.DataFrame(timeF)


    ###### Calcul des moyennes (quantités et times)

    # Pour les quantités
    Q = quantity.to_numpy(dtype='float')
    Q = np.cumsum(Q)

    avgQ = []
    for x in range(len(Q)):
        i = x+1
        y = [float(Q[x]) / i]
        avgQ.append(y)

    avgQ = pd.DataFrame(avgQ)

    # Pour les times
    T = timeF.to_numpy(dtype='float')
    T = np.cumsum(T)

    avgT = []
    for x in range(len(Q)):
        i = x+1
        y = [float(T[x]) / i]
        avgT.append(y)

    avgT = pd.DataFrame(avgT)


    ##### Recupération du numero de la semaine à partir de de la date de
    ##### debut de ravitaillement
    Week_number = pd.to_datetime(Date_quantite1['Date_debut'].squeeze())
    Week_number = Week_number.dt.week
    Week_number = pd.DataFrame(Week_number)


    ##### Construction de la dataFrame finale
    #result = np.column_stack((Projet, No_Dispenser, Dispenser_Type, Dispenser_Pressure, date1, date2, Week_number, quantity, timeF, avgQ, avgT))
    result = np.concatenate([Projet, No_Dispenser, Dispenser_Type, Dispenser_Pressure, date1, date2, Week_number, quantity, timeF, avgQ, avgT], axis=1)
    result = pd.DataFrame(result)
    result.columns = [['Projet', 'No_Dispenser', 'Dispenser_Type', 'Dispenser_Pressure', 'Distribution_Date_Start','Distribution_Date_End','Week_number','Quantity',
                       'TimeMin', 'AVG_Quantity', 'AVG_Time']]
    
    # Ecriture du premier fichier csv en local
    result.to_csv(OutData, index=False)
    #####################################################################################
    
    
    """Dans cette section nous utilisons la dataframme 2 des pressions chargée plus haut
        et la dataframe result pour construire la colonne des pressions max de chaque
            intervalle de temps (start - end)
    """    
    
    # Lecture du premier fichier en vu de l'exploiter 
    result_copy1 = pd.read_csv(OutData)
   
    # Suppression des colonnes inutiles pour alléger les filtres
    result_copy1.drop(['Projet', 'No_Dispenser', 'Dispenser_Type', 'Dispenser_Pressure', 'Quantity', 'TimeMin', 'AVG_Quantity','AVG_Time', 'Week_number'], 1, inplace=True)
    data2.drop(['TagId', 'Hour', 'Tagname', 'Month', 'Extrainfo'], 1, inplace=True)
    
    data2['TimeStr'] = pd.to_datetime(data2['TimeStr'], errors='coerce')
    data2['TimeStr'] = data2['TimeStr'].dt.strftime("%Y-%m-%d %H:%M")
    

    pression_max = []
    for i in result_copy1.index:
        a = result_copy1['Distribution_Date_Start'][i]
        a = pd.to_datetime(a, errors='coerce')
        a = a.strftime("%Y-%m-%d %H:%M")
        b = result_copy1['Distribution_Date_End'][i]
        b = pd.to_datetime(b, errors='coerce')
        b = b.strftime("%Y-%m-%d %H:%M")
        filtered_df = data2.loc[data2['TimeStr'] > a]
        filtered_df = filtered_df.loc[filtered_df['TimeStr'] <= b]
        filtered_df = filtered_df['Value'].max()
        pression_max.append(filtered_df)

    pression_max = pd.DataFrame(pression_max, columns=['Pression_max_distribution'])
    
    # Concatenation dataframe FWD et colonne PT
    result = np.column_stack((result,pression_max))
    result = pd.DataFrame(result)
    #result.columns = [['Projet','Distribution_Date_Start','Distribution_Date_End','Week_number','Quantity',
         #              'TimeMin', 'AVG_Quantity', 'AVG_Time','Pression_max_distribution']]
    ###################################################################################
    
    
    
        
    """Dans cette section nous utilisons la dataframme 3 des pressions cible chargées plus haut
        et la dataframe result pour construire la colonne des pression target distribution de chaque
            intervalle de temps (start - end)
    """    
    
        
    # Lecture du premier fichier en vu de l'exploiter 
    result_copy2 = pd.read_csv(OutData)
   
    # Suppression des colonnes inutiles pour alléger les filtres
    result_copy2.drop(['Projet', 'No_Dispenser', 'Dispenser_Type', 'Dispenser_Pressure', 'Quantity', 'TimeMin', 'AVG_Quantity','AVG_Time', 'Week_number'], 1, inplace=True)
    data3.drop(['TagId', 'Hour', 'Tagname', 'Month', 'Extrainfo'], 1, inplace=True)
    
    # Suppression de la partie secondes des dates
    data3['TimeStr'] = pd.to_datetime(data3['TimeStr'], errors='coerce')
    data3['TimeStr'] = data3['TimeStr'].dt.strftime("%Y-%m-%d %H:%M")
    
    pression_cible = []
    for i in result_copy2.index:
        a = pd.to_datetime(result_copy2['Distribution_Date_Start'][i], errors='coerce')
        a = a.strftime("%Y-%m-%d %H:%M")
        b = pd.to_datetime(result_copy2['Distribution_Date_End'][i], errors='coerce')
        b = b.strftime("%Y-%m-%d %H:%M")
        filtered_df = data3.loc[data3['TimeStr'] > a]
        filtered_df = filtered_df.loc[filtered_df['TimeStr'] <= b]
        filtered_df = filtered_df['Value'].max() * 10
        if pd.isnull(filtered_df):
            filtered_df = pression_defaut
        pression_cible.append(filtered_df)
        
    

    pression_cible = pd.DataFrame(pression_cible, columns=['Pression_Target_distribution'])
    
    
    # Construction colonne Pinit
    l1 = ligne - len(data4['Value'])
    if l1 != 0:
        vect1 = np.zeros(l1)
        vect1 = pd.DataFrame(vect1)
        pinit = data4['Value']
        pinit = pd.DataFrame(pinit)
        pinit = pd.concat([vect1,pinit],axis=0)
        pinit.reset_index(inplace=True, drop=True)
        pinit.drop(pinit.columns[[0]], axis = 1, inplace = True) 
    else:
        pinit = data4['Value']
        pinit = pd.DataFrame(pinit)
        pinit.reset_index(inplace=True, drop=True)
    
    
    # Construction de la colonne Refueling end type
    l2 = ligne - len(data5['Value'])
    if l2 != 0:
        vect2 = np.zeros(l2)
        vect2 = pd.DataFrame(vect2)
        refg = data5['Value']
        refg = pd.DataFrame(refg)
        refg = pd.concat([vect2, refg], axis=0)
        refg.reset_index(inplace=True, drop=True)
        refg.drop(refg.columns[[0]], axis=1, inplace=True)
    else:
        refg = data5['Value']
        refg = pd.DataFrame(refg)
        refg.reset_index(inplace=True, drop=True)
    
    
    # Construction de la colonne Communication type
    l3 = ligne - len(data6['Value'])
    if l3 != 0:
        vect3 = np.zeros(l3)
        vect3 = pd.DataFrame(vect3)
        comtyp = data6['Value']
        comtyp = pd.DataFrame(comtyp)
        comtyp = pd.concat([vect3, comtyp], axis=0)
        comtyp.reset_index(inplace=True, drop=True)
        comtyp.drop(comtyp.columns[[0]], axis=1, inplace=True)
    else:
        comtyp = data6['Value']
        comtyp = pd.DataFrame(comtyp)
        comtyp.reset_index(inplace=True, drop=True)
    
    
    # Concatenation dataframe FWD et colonne PT
    result = np.column_stack((result,pression_cible))
    result = pd.DataFrame(result)
    result = pd.concat([result,pinit,refg,comtyp], axis=1, ignore_index=True)
    result.columns = [['Projet', 'No_Dispenser', 'Dispenser_Type', 'Dispenser_Pressure','Distribution_Date_Start','Distribution_Date_End','Week_number','Quantity',
                       'TimeMin', 'AVG_Quantity', 'AVG_Time','Pression_max_distribution', 'Pression_Target_distribution', 
                       'Pression_initiale_distribution', 'Refueling_end_type','Communication_type']]
    ###################################################################################


    ##### Ecriture du fichier csv final
    #returnValue1 = result.to_csv(OutData, sep=',', index=False)
    returnValue2 = result.to_csv('/home/pi/Desktop/DataHistorisationAPP/Reports/Historique Distributions/'+projet+'.csv', sep=',', index=False, encoding='utf-8')
    print(result)
    os.remove(OutData)


    
# Appliqué à la station HRS-IP1 D1
CleanData('/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS-IP1/Year 2022/FT 4.201 TotalMassFwd.csv', 
          '/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS-IP1/Year 2022/PT 4.201a.csv',
          '/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS-IP1/Year 2022/PTarget D1.csv',
          '/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS-IP1/Year 2022/LDV Pinit.csv',
          '/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS-IP1/Year 2022/LDV Refueling End Type.csv',
          '/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS-IP1/Year 2022/LDV D1 CommunicationType.csv',
          'frame1.csv', 'HRS-IP1 D1', 1, 'LDV', 'H35') 

# Appliqué à la station HRS-IP1 D2
CleanData('/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS-IP1/Year 2022/FT 4.101 TotalMassFwd.csv', 
          '/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS-IP1/Year 2022/PT 4.101a.csv',
          '/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS-IP1/Year 2022/PTarget D2.csv',
          '/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS-IP1/Year 2022/HDV Pinit.csv',
          '/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS-IP1/Year 2022/HDV Refueling End Type.csv',
          '/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS-IP1/Year 2022/HDV D2 CommunicationType.csv',
          'frame2.csv', 'HRS-IP1 D2', 2, 'HDV', 'H35')


# Appliqué à la station HRS-H2M
CleanData('/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS-H2M/Year 2022/IDC D1 MASS FWD.csv', 
          '/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS-H2M/Year 2022/PT 8.202a.csv',
          '/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS-H2M/Year 2022/D1 PTarget.csv',
          '/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS-IP1/Year 2022/HDV Pinit.csv',
          '/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS-IP1/Year 2022/HDV Refueling End Type.csv',
          '/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS-IP1/Year 2022/HDV D2 CommunicationType.csv',
          'frame3.csv', 'HRS-H2M', 1, 'LDV', 'H70')


# Appliqué à la station HRS1-SMTAG D1
CleanData('/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS1-SMTAG/Year 2022/MES EANA DEBIT FT8b D1 TotalMassFwd.csv', 
          '/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS1-SMTAG/Year 2022/MES EANA STANDARDS D1 AI Dx PT aval b.csv',
          '/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS1-SMTAG/Year 2022/D1 HDV PTarget.csv',
          '/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS1-SMTAG/Year 2022/D1 HDV Pression Initial.csv',
          '/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS1-SMTAG/Year 2022/D1 HDV Refueling End Type.csv',
          '/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS1-SMTAG/Year 2022/D1 HDV Communication Type.csv',
          'frame4.csv', 'HRS1-SMTAG D1', 1, 'HDV', 'H35')


# Appliqué à la station HRS1-SMTAG D2
CleanData('/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS1-SMTAG/Year 2022/MES EANA DEBIT FT8b D2 TotalMassFwd.csv', 
          '/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS1-SMTAG/Year 2022/MES EANA STANDARDS D2 AI Dx PT aval b.csv',
          '/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS1-SMTAG/Year 2022/D2 HDV PTarget.csv',
          '/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS1-SMTAG/Year 2022/D2 HDV Pression Initial.csv',
          '/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS1-SMTAG/Year 2022/D2 HDV Refueling End Type.csv',
          '/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS1-SMTAG/Year 2022/D2 HDV Communication Type.csv',
          'frame5.csv', 'HRS1-SMTAG D2', 2, 'HDV', 'H35')


# Appliqué à la station HRS-APEX (D1)
CleanData('/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS-APEX/Year 2022/TotalMASSFWD D1.csv', 
          '/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS-APEX/Year 2022/PT 4.201b.csv',
          '/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS-APEX/Year 2022/D1 LDV PTarget.csv',
          '/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS-APEX/Year 2022/D1 LDV Pression Initial.csv',
          '/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS-APEX/Year 2022/D1 LDV Refueling End Type.csv',
          '/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS-APEX/Year 2022/D1 LDV Communication Type.csv',
          'frame6.csv', 'HRS-APEX D1', 1, 'LDV', 'H35')


# Appliqué à la station HRS-APEX (D2)
# CleanData('/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS-APEX/Year 2022/TotalMASSFWD D2.csv', 
#           '/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS-APEX/Year 2022/PT 4.102b.csv',
#           '/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS-APEX/Year 2022/D2 HDV PTarget.csv',
#           '/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS-APEX/Year 2022/D2 HDV Pression Initial.csv',
#           '/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS-APEX/Year 2022/D2 HDV Refueling End Type.csv',
#           '/home/pi/Desktop/DataHistorisationAPP/DataStorage/HRS-APEX/Year 2022/D2 HDV Communication Type.csv',
#           'frame7.csv', 'HRS-APEX D2', 2, 'HDV', 'H35')