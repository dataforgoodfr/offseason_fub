import os.path
from tokenize import group

import pandas as pd

from lecture_ecriture_donnees import preview_file, write_csv_on_s3
from nettoyage_donnees import get_commune_name_from_insee


def compute_notes(df, insee_refs, group_of_questions, save_fold, commune_id="insee", commune_type_id="Catégorie Baromètre",
                                                                    avg_note_att_name="average_note"):
    """Calcule les notes des communes, ce qui inclut la note moyenne à l'ensemble des questions d'évaluation, mais également les notes
    myennes des différentes catégories de questions. Les notes sont ensuite sauvegardées, par ordre décroissant de note moyenne,
    dans 6 fichiers csv, chacun étant associé à une catégorie du baromètre (grandes villes, villes moyennes ...)

    ENTREES :
    - df (pd.DataFrame) : jeu de données (préférablement nettoyé)
    - insee_refs (pd.DataFrame). Tableau pandas associant les codes INSEE au nom de commune et autres caractéristiques de la commune
    - group_of_questions (dict) : dictionnaire dans lequel les clés sont les noms des groupes de questions et les valeurs
            les listes contenant les questions associées
    - save_fold : chemin ou les fichiers csv regoupant les notes sont sauvegardés

    SORTIES :
        notes_df (pd.DataFrame) Tableau  pandas regroupans les notes"""

    note_tables_columns = ["INSEE", "Commune", "Nombre de réponses (après filtrage)", "Note moyenne", "Type de commune",
                           *group_of_questions.keys()]
    insee_codes = df[commune_id].unique()
    types_of_communes = insee_refs[commune_type_id].unique()
    notes_df = {categorie:pd.DataFrame(columns=note_tables_columns) for categorie in types_of_communes}
    not_found_insee_codes = []
    for insee_code in insee_codes:
        df_commune = df[df[commune_id] == insee_code]
        nom_commune, categorie, not_found = get_commune_name_from_insee(insee_code, insee_refs)
        if not_found:
            not_found_insee_codes.append(insee_code)
        nouvelle_ligne = [insee_code, nom_commune, len(df_commune), round(df_commune[avg_note_att_name].values.mean(),2), categorie]
        for group in group_of_questions.keys():
            average_note = df_commune[group_of_questions[group]].values.mean()
            nouvelle_ligne.append(round(average_note,2))
        if categorie in notes_df.keys():
            notes_df[categorie].loc[len(notes_df[categorie])] = nouvelle_ligne
    print("les codes insee suivants n'ont pas été trouvés dans le tableau des communes", not_found_insee_codes)
    if not os.path.exists(save_fold):
        os.makedirs(save_fold)
    for categorie in notes_df.keys():
        notes = notes_df[categorie]
        notes = notes.sort_values(by="Note moyenne", ascending=False)
        notes.to_csv(f"{save_fold}/note_communes_{categorie}.csv", index=False)
    return notes_df






if __name__ == '__main__':
    data_2025 = False # = True si on souhaite utiliser les données de 2025, =False si on souhaite utiliser les données de 2021
    insee_refs = preview_file(key="data/converted/2025/brut/220128_BV_Communes_catégories.csv", csv_sep=",", nrows=None)
    print('columns insee refs', insee_refs.columns)

    filtered_data_key = "data/converted/2025/nettoyee/reponses-2025-04-29-filtered.csv" if data_2025 else \
                        "data/reproduced/2021/reponses-2021-12-01-08-00-00_filtered_2025_method.csv"
    df = preview_file(filtered_data_key, nrows=None)
    commune_id = "insee" if data_2025 else "q01"
    group_of_questions = {'Services et stationnement': [f"q{i}" for i in range(29, 33)],
                          'Efforts de la Commune': [f"q{i}" for i in range(25, 29)],
                          'Confort': [f"q{i}" for i in range(20, 25)],
                          'Securité': [f"q{i}" for i in range(14, 20)],
                          'Ressenti général': [f"q{i}" for i in range(7, 14)]} if data_2025 else \
                        {'Services et stationnement': [f"q{i}" for i in range(35, 40)],
                          'Efforts de la Commune': [f"q{i}" for i in range(31, 35)],
                          'Confort': [f"q{i}" for i in range(26, 31)],
                          'Securité': [f"q{i}" for i in range(20, 26)],
                          'Ressenti général': [f"q{i}" for i in range(14, 20)]}

    #save_key_s3 ="data/converted/2025/nettoyee/notes_communes.csv"
    save_fold = "../barometre_notes/notes_2025" if data_2025 else "../barometre_notes/notes_2021"
    notes_df = compute_notes(df, insee_refs, group_of_questions, save_fold, commune_id)


