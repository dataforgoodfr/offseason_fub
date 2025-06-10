import numpy as np
import matplotlib.pyplot as plt
from lecture_donnees import preview_file

insee_refs = preview_file(key="data/converted/2025/brut/220128_BV_Communes_catégories.csv", csv_sep=",", nrows=None)
data_2021 = preview_file(key="data/converted/2021/brut/reponses-2021-12-01-08-00-00.csv", nrows=None)

print('columns insee refs', insee_refs.columns)
print('data shape', data_2021.shape)
#print('col insee', insee_refs.columns)

email_id = "q56" #q56 = emails pour les données de 2021
emails = data_2021[email_id]
commune_id = "q01"#q01 = communes pour les données de 2021
insee_codes = data_2021[commune_id].unique()
questions_to_average = [f"q{i}" for i in range(14,41)] # questions associées à l'ensemble des critères d'évalutations

group_of_questions = {'Stationnement':[f"q{i}" for i in range(35,40)],
                      'Effort Commune':[f"q{i}" for i in range(31,35)],
                      'Confort':[f"q{i}" for i in range(26,31)],
                      'Securité':[f"q{i}" for i in range(20,26)],
                       'Ressenti':[f"q{i}" for i in range(14,20)]}

not_found_insee_codes = []
for insee_code in insee_codes:
    try:
        nom_commune = insee_refs.loc[insee_refs["INSEE"]==insee_code, "Commune"].item() # récupère le nom de la commune associée au code INSEE
    except:
        print(f"Le numéro INSEE {insee_code} n'a pas été trouvé dans le tableau des communes")
        nom_commune = insee_code
        not_found_insee_codes.append(insee_code)
    df_commune = data_2021[data_2021[commune_id] == insee_code].copy()
    # moyennage de l'ensemble des critères d'évaluation
    df_commune["average_notes"] = df_commune[questions_to_average].mean(axis=1)
    print('Nombre de réponses avant filtrage', len(df_commune["average_notes"]))
    # retire les notes moyennes 1 et 6.  Si une seule réponse, on la grade pour éviter de moyenner un tableau vide
    filtered = df_commune[(df_commune["average_notes"] >1) & (df_commune["average_notes"]<6)] if len(df_commune)>1 else df_commune
    # retire les notes >5 ou <2 si l'adresse mail n'est pas renseignée. Si une seule réponse, on la grade pour éviter de moyenner un tableau vide
    filtered = filtered[(filtered[email_id].notna()) | ((filtered["average_notes"]>=2) & (filtered["average_notes"]<=5))] if len(filtered) > 1 else filtered
    mean_avg_notes, std_avg_notes = filtered["average_notes"].mean(), np.nan_to_num(filtered["average_notes"].std()) # std peut etre egal à 0 sil y a un seul élément dans le tableau
    # calcul des deuxièmes moyennes et ecart type après avoir supprimé les réponses au dela de moyenne +- 2 écarts types
    filtered2 = filtered[(filtered["average_notes"] >= mean_avg_notes - 2*std_avg_notes)
                         & (filtered["average_notes"] <= mean_avg_notes + 2*std_avg_notes)]
    new_mean, new_std = filtered2["average_notes"].mean(), np.nan_to_num(filtered2["average_notes"].std())
    #suppression des réponses au dela de moyenne +- 2.5 écart-type
    filtered = filtered[(filtered["average_notes"] >= new_mean - 2.5*new_std)
                         & (filtered["average_notes"] <= new_mean + 2.5*new_std)]
    # supperssion des emails doublons (si une personne répond 2 fois)
    # print('len filtered 3', len(filtered))
    # filtered = filtered.drop_duplicates(subset=[email_id], keep='first') # not working because a lot of NaN values for email (detected as identical)
    print('Nombre de réponses après filtrage', len(filtered))
    for group in group_of_questions.keys():
        average_note = filtered[group_of_questions[group]].values.mean()
        print(f'Commune {nom_commune}, category {group}, note {round(average_note,3)}')
    """
    plt.hist(average_notes)
    plt.xlabel('Notes moyennes')
    plt.title(f'Histogramme de la commune {insee_code}')
    plt.show()
    """

print("les codes insee suivants n'ont pas été trouvés dans le tableau des communes", not_found_insee_codes)

