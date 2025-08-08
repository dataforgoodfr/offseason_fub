process_bdd_nettoyee.py

"""
Script de traitement de la base de données nettoyée : intégration des données INSEE, 
ajout des catégories de croisement selon les critères de la FUB, suppression des données 
personnelles sensibles. 

La base produite servira de support aux analyses avancées.

valeurs ciblées 
    #Note globale moyenne (average_note)
    #Services et stationnement
    #Efforts de la Commune
    #Confort
    #Securité
    #Ressenti général
Catégories de croisement 		
    #Age 	(q48)
    #Genre 	(q47) 	
    #QPV 	(q4) 1 -  Habitant QPV 2 - Habitant hors QPV	
    #Fréquence de circulation dans la commune - FCC(q6) 	
    	FCC1: 1/ Tous les jours ou presque  
    	2 / 1 à 3 fois par semaine 3/ 1 à 3 fois par mois 4/ 1 à 3 fois par an  5/ Jamais 	
    	FCC2: Quotidiennement (1/2) Ponctuellement (3/4)
    #Niveau de pratique	(q37)  Utilisation uniquement de la classe agrégée 	
    	Débutant (1/2) Intermédiaire (3/4) Confirmé (5/6)
    #Multimodalité - l'abonnement au TC (q45) Utilisation uniquement de la classe agrégée 	
    	Abonné.e TC (1/2/3) ; Non abonné.e (4)
    #Par type de territoire 	1. Grandes Villes 2. Communes de banlieue 3. Petites Villes 
    	4.Bourgs et Villages 5. Villes Moyennes	
    #Par région 	Les seizes régions  	
    #Par département 	Les 101 Départements 
    #Par EPCI 	Les 1301 EPCIA 

"""

import pandas as pd
import numpy as np
from utils import get_commune_name_from_insee
from lecture_ecriture_donnees import preview_file, make_dir, write_csv_on_s3

data = preview_file(key="data/converted/2025/nettoyee/250604_Export_Reponses_Final_Result_Nettoyee.csv", csv_sep=";", nrows=None)
insee_refs = preview_file(key="data/converted/2025/brut/220128_BV_Communes_catégories.csv", csv_sep=",", nrows=None)

columns_to_keep = ['uid', 'email', 'insee', 'q47', 'q48', 'q4', 'q6', 'q7', 'q29', 'q30', 'q31', 'q32', 'q25', 'q26', 'q27', 'q28', 'q20', 
                   'q21', 'q22', 'q23', 'q24', 'q14', 'q15', 'q16', 'q17', 'q18', 'q19', 'q7', 'q8', 'q9', 'q10', 'q11', 
                   'q12', 'q13', 'q37', 'q45', 'average_note']

# Premier filtrage
df = data[columns_to_keep].copy()

#Age1 & Age2
#age1 q48     0 - Moins de 11 ans; 1- 11-14 ans; 2- 15-18 ans; 3-18 - 24 ans; 4-25 - 34 ans; 
            # 5-35 - 44 ans; 6-45 - 54 ans; 7-55 - 64 ans; 8-65 - 75 ans; 9-Plus de 75 ans
#age2 q48 1 - Jeune (0-18ans) ; 2 - Adulte (18-54 ans) 3 - Senior (+ 54 ans) 
df['q48'] = df['q48'].astype('Int64') 

map_age_group1 = {
    0: '-11 ans',
    1: '11-14 ans',
    2: '15-18 ans',
    3: '18-24 ans',
    4: '25-34 ans',
    5: '35-44 ans',
    6: '45-54 ans',
    7: '54-64 ans',
    8: '65-75 ans',
    9: '+75 ans'
} 

df.loc[:, 'age1'] = df['q48'].map(map_age_group1).astype('string')

def map_age_group2(x):
    if pd.isna(x):
        return np.nan
    elif 0 <= x <= 2:
        return 'Jeune'
    elif 3 <= x <= 6:
        return 'Adulte'
    elif 7 <= x <= 9:
        return 'Senior'
    else:
        return np.nan  

df.loc[:, 'age2'] = df['q48'].apply(map_age_group2) 

#Genre
#q47 1- F 2- H 3 - X
df['q47'] = df['q47'].astype('Int64') 
df.loc[:,'genre'] = df['q47'].map({1: 'F', 2: 'M', 3: 'X'})

#QPV
#Habitant QPV - Habitant hors QPV
df.loc[:, 'QPV'] = np.where(df['q4'].isna(), 'Hors QPV', 'Habitant QPV')

#FCC1 - Fréquence de circulation dans la commune (q6) 
#1/ Tous les jours ou presque  
#2 / 1 à 3 fois par semaine 
#3/ 1 à 3 fois par mois 
#4/ 1 à 3 fois par an  
#5/ Jamais 	

#Frequence CC2
#Quotidiennement (1/2) 
#Ponctuellement (3/4)

df['q6'] = df['q6'].astype('Int64') 

map_FCC1 = {
    1: 'Tous les jours',
    2: '1 à 3 fois par semaine',
    3: '1 à 3 fois par mois',
    4: '1 à 3 fois par ans',
    5: 'Jamais',
} 

df.loc[:, 'FCC1'] = df['q6'].map(map_FCC1).astype('string')

def map_FCC2(x):
    if pd.isna(x):
        return np.nan
    elif 1 <= x <= 2:
        return 'Quotidiennement'
    elif 3 <= x <= 4:
        return 'Ponctuellement'
    elif x == 5:
        return 'Jamais'
    else:
        return np.nan  

df.loc[:, 'FCC2'] = df['q6'].apply(map_FCC2).astype('string') 

#Niveau de pratique	(q37)  Utilisation uniquement de la classe agrégée 	Débutant (1/2) Intermédiaire (3/4) Confirmé (5/6)
def map_NP(x):
    if pd.isna(x):
        return np.nan
    elif 1 <= x <= 2:
        return 'Débutant'
    elif 3 <= x <= 4:
        return 'Intermédiaire'
    elif 5 <= x <= 6:
        return 'Confirmé'
    else:
        return np.nan  

df.loc[:, 'NiveauPratique'] = df['q37'].apply(map_NP).astype('string') 

#Multimodalité - l'abonnement au TC (q45) (Q45)	Utilisation uniquement de la classe agrégée 	Abonné.e TC (1/2/3) ; Non abonné.e (4)
def map_TC(x):
    if pd.isna(x):
        return np.nan
    elif 1 <= x <= 3:
        return 'Abonné.e TC'
    elif x == 4:
        return 'Non abonné.e'
    else:
        return np.nan  

df.loc[:, 'abonnementTC'] = df['q45'].apply(map_TC).astype('string') 

#Valeurs ciblées :
#Service et stationement 
#Efforts de la commune
#Confort
#Securite
#Ressenti general

group_of_questions = {'Services et stationnement': [f"q{i}" for i in range(29, 33)],
                              'Efforts de la Commune': [f"q{i}" for i in range(25, 29)],
                              'Confort': [f"q{i}" for i in range(20, 25)],
                              'Securité': [f"q{i}" for i in range(14, 20)],
                              'Ressenti général': [f"q{i}" for i in range(7, 14)]} 

for group_name, question_cols in group_of_questions.items():
    # Calculate the row-wise mean of the specified columns
    df[group_name] = df[question_cols].mean(axis=1)

#Donnees INSEE:
#Par type de territoire 	1. Grandes Villes 2. Communes de banlieue 3. Petites Villes 4.Bourgs et Villages 5. Villes Moyennes	
#Par région 	Les seizes régions 	
#Par département 	Les 101 Départements 
#Par EPCI 	Les 1301 EPCIA 

df['insee'] = df['insee'].astype(str)
insee_refs['INSEE'] = insee_refs['INSEE'].astype(str)

df_merged = df.merge(
    insee_refs[['INSEE', 'Commune', 'DEP', 'REG', 'TYPE_COM', 'Catégorie Baromètre', 'EPCI', 'Réponses de cyclistes' ]],
    left_on='insee',
    right_on='INSEE',
    how='left'
)


#save merged to csv & upload to S3
file_csv = '250604_Export_Reponses_Final_Result_Nettoyee_Processed.csv'
df_merged.to_csv(file_csv, index=False, sep=';')
save_path = 'data/converted/2025/nettoyee/processed/250604_Export_Reponses_Final_Result_Nettoyee_Processed.csv'  
write_csv_on_s3(df_merged, save_path)

#consolidated path for EDA
save_path = 'data/DFG/2025/data_num/250604_Export_Reponses_Final_Result_Nettoyee_Processed.csv'  
write_csv_on_s3(df_merged, save_path)

#Remove sensitive information and q columns
columns_to_remove = ['email', 'insee', 'q47', 'q48', 'q4', 'q6', 'q7', 'q29', 'q30', 'q31',
       'q32', 'q25', 'q26', 'q27', 'q28', 'q20', 'q21', 'q22', 'q23', 'q24',
       'q14', 'q15', 'q16', 'q17', 'q18', 'q19', 'q7', 'q8', 'q9', 'q10',
       'q11', 'q12', 'q13', 'q37', 'q45']
df_filtered = df_merged.drop(columns=columns_to_remove)

#save filtered to csv & upload to S3
file_csv = 'données2025_traitées_nettoyées_anonymisées.csv'
df_filtered.to_csv(file_csv, index=False, sep=';')
save_path = 'data/converted/2025/nettoyee/processed/données2025_traitées_nettoyées_anonymisées.csv'  
write_csv_on_s3(df_filtered, save_path)

#consolidated path for EDA
save_path = 'data/DFG/2025/data_num/données2025_traitées_nettoyées_anonymisées.csv'  
write_csv_on_s3(df_filtered, save_path)

