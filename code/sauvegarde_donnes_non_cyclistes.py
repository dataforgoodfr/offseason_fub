from lecture_ecriture_donnees import preview_file, write_csv_on_s3
"""petit code pour sauvegarder un extract de la base avec uniquement les données des non cyclistes"""

data = preview_file(key="data/converted/2025/brut/250604_Export_Reponses_Brut_Final_Result 1.csv", nrows=None, csv_sep=",")

data_non_cyclistes = data[data["q14"].isna()]

print('col', data_non_cyclistes.columns)

q_to_keep = ["insee"]
q_to_keep = q_to_keep + [f'q{i}' for i in range(49, 58)]
data_non_cyclistes = data_non_cyclistes[q_to_keep]


save_key = "data/converted/2025/nettoyee/250604_Export_Reponses_Final_Result_Non_Cyclistes.csv"
write_csv_on_s3(data_non_cyclistes, save_key)