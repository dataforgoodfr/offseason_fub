import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

from lecture_ecriture_donnees import preview_file, write_csv_on_s3


def filter_one_commune(df_commune, questions_to_average, email_id, avg_note_att_name="average_note"):
    # moyennage de l'ensemble des critères d'évaluation
    df_commune[avg_note_att_name] = df_commune[questions_to_average].mean(axis=1)
    print('Nombre de réponses avant filtrage', len(df_commune[avg_note_att_name]))
    # retire les notes moyennes 1 et 6.  Si une seule réponse, on la grade pour éviter de moyenner un tableau vide
    filtered = df_commune[(df_commune[avg_note_att_name] > 1) & (df_commune[avg_note_att_name] < 6)] if len(
        df_commune) > 1 else df_commune
    # retire les notes >5 ou <2 si l'adresse mail n'est pas renseignée. Si une seule réponse, on la grade pour éviter de moyenner un tableau vide
    filtered = filtered[
        (filtered[email_id].notna()) | ((filtered[avg_note_att_name] >= 2) & (filtered[avg_note_att_name] <= 5))] if len(
        filtered) > 1 else filtered
    mean_avg_notes, std_avg_notes = filtered[avg_note_att_name].mean(), np.nan_to_num(
        filtered[avg_note_att_name].std())  # std peut etre egal à 0 sil y a un seul élément dans le tableau
    # calcul des deuxièmes moyennes et ecart type après avoir supprimé les réponses au dela de moyenne +- 2 écarts types
    filtered2 = filtered[(filtered[avg_note_att_name] >= mean_avg_notes - 2 * std_avg_notes)
                         & (filtered[avg_note_att_name] <= mean_avg_notes + 2 * std_avg_notes)]
    new_mean, new_std = filtered2[avg_note_att_name].mean(), np.nan_to_num(filtered2[avg_note_att_name].std())
    # suppression des réponses au dela de moyenne +- 2.5 écart-type
    filtered = filtered[(filtered[avg_note_att_name] >= new_mean - 2.5 * new_std)
                        & (filtered[avg_note_att_name] <= new_mean + 2.5 * new_std)]
    # supperssion des emails doublons (si une personne répond 2 fois)
    # print('len filtered 3', len(filtered))
    # filtered = filtered.drop_duplicates(subset=[email_id], keep='first') # not working because a lot of NaN values for email (detected as identical)
    print('Nombre de réponses après filtrage', len(filtered))
    return filtered

def filter_data_set(df, questions_to_average, email_id, commune_id, save_key):
    insee_codes = df[commune_id].unique()
    all_filtered_data = []
    for insee_code in insee_codes:
        df_commune = df[df[commune_id] == insee_code].copy()
        # moyennage de l'ensemble des critères d'évaluation
        filtered = filter_one_commune(df_commune, questions_to_average, email_id)
        all_filtered_data.append(filtered)
    all_filtered_data = pd.concat(all_filtered_data, ignore_index=True)
    write_csv_on_s3(all_filtered_data, save_key)
    return all_filtered_data

def get_commune_name_from_insee(insee_code, insee_refs, not_found_insee_codes):
    nom_commune = insee_refs.loc[
        insee_refs["INSEE"] == insee_code, "Commune"]  # récupère le nom de la commune associée au code INSEE
    categorie = insee_refs.loc[insee_refs["INSEE"] == insee_code, "Catégorie Baromètre"]
    if len(nom_commune) == 0:
        print(f"Le numéro INSEE {insee_code} n'a pas été trouvé dans le tableau des communes")
        nom_commune = insee_code
        categorie = 'Not found'
        not_found_insee_codes.append(insee_code)
    else:
        nom_commune = nom_commune.item()
        categorie = categorie.item()
    return nom_commune, categorie



if __name__ == '__main__':
    data_2021 = preview_file(key="data/converted/2021/brut/reponses-2021-12-01-08-00-00.csv", nrows=None)
    print('loaded')
    # print('col insee', insee_refs.columns)

    email_id = "q56"  # q56 = emails pour les données de 2021
    emails = data_2021[email_id]
    commune_id = "q01"  # q01 = communes pour les données de 2021
    questions_to_average = [f"q{i}" for i in
                            range(14, 41)]  # questions associées à l'ensemble des critères d'évalutations


    save_key = "data/reproduced/2021/reponses-2021-12-01-08-00-00_filtered.csv"
    all_filtered_data = filter_data_set(data_2021, questions_to_average, email_id, commune_id, save_key)
    print('filtered data shape', all_filtered_data.shape)
    all_filtered_data.to_csv("/home/thibaut/filtered.csv", index=False)




