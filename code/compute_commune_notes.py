import pandas as pd

from lecture_ecriture_donnees import preview_file, write_csv_on_s3
from nettoyage_donnees import get_commune_name_from_insee


def compute_notes(df, insee_refs, group_of_questions, commune_id, save_path, avg_note_att_name="average_note"):
    note_tables_columns = ["INSEE", "Commune", "Nombre de réponses (après filtrage)", "Note moyenne", "Type de commune",
                           *group_of_questions.keys()]
    notes_df = pd.DataFrame(columns=note_tables_columns)
    insee_codes = df[commune_id].unique()
    not_found_insee_codes = []
    for insee_code in insee_codes:
        df_commune = df[df[commune_id] == insee_code]
        nom_commune, categorie = get_commune_name_from_insee(insee_code, insee_refs, not_found_insee_codes)
        nouvelle_ligne = [insee_code, nom_commune, len(df_commune), round(df_commune[avg_note_att_name].values.mean(),2), categorie]
        for group in group_of_questions.keys():
            average_note = df_commune[group_of_questions[group]].values.mean()
            nouvelle_ligne.append(round(average_note,2))
        notes_df.loc[len(notes_df)] = nouvelle_ligne
    print("les codes insee suivants n'ont pas été trouvés dans le tableau des communes", not_found_insee_codes)
    write_csv_on_s3(notes_df, save_path)
    return notes_df






if __name__ == '__main__':
    insee_refs = preview_file(key="data/converted/2025/brut/220128_BV_Communes_catégories.csv", csv_sep=",", nrows=None)
    print('columns insee refs', insee_refs.columns)
    df = preview_file("data/reproduced/2021/reponses-2021-12-01-08-00-00_filtered.csv", nrows=None)

    commune_id = "q01"  # q01 = communes pour les données de 2021
    group_of_questions = {'Services et stationnement': [f"q{i}" for i in range(35, 40)],
                          'Efforts de la Commune': [f"q{i}" for i in range(31, 35)],
                          'Confort': [f"q{i}" for i in range(26, 31)],
                          'Securité': [f"q{i}" for i in range(20, 26)],
                          'Ressenti général': [f"q{i}" for i in range(14, 20)]}

    notes_df = compute_notes(df, insee_refs, group_of_questions, commune_id, "data/reproduced/2021/computed_notes.csv")
    notes_df.to_csv("/home/thibaut/note_communes.csv", index=False)


