from local_paths import your_local_save_fold, make_dir
from lecture_ecriture_donnees import preview_file
from nettoyage_donnees import get_commune_name_from_insee
import numpy as np
import matplotlib.pyplot as plt

"""petit code pour tracer les histogrammes des données netoyées de 2021 (dans le but de pouvoir les comparer avec ceux obtenus par la nouvelle méthode)"""

df = preview_file(key="data/converted/2021/nettoye/Réponses post-fraude_Result 1.csv", csv_sep=",", nrows=None)
insee_refs = preview_file(key="data/converted/2025/brut/220128_BV_Communes_catégories.csv", csv_sep=",", nrows=None)

insee_codes = ["80164", "39198", "94078", "62263"]


questions_to_average = [f"q{i}" for i in range(14, 41)]
start, stop, step = 1, 6, 0.2

save_fold = f"{your_local_save_fold}/histograms/histograms_2021_with_2021_method"
make_dir(save_fold)

for insee_code in insee_codes:
    nom_commune, _, _ = get_commune_name_from_insee(insee_code, insee_refs)
    df_commune = df[df["q01"] == insee_code].copy()
    df_commune["average_note"] = df_commune[questions_to_average].mean(axis=1)

    bins = np.arange(start, stop + step, step)
    plt.figure(figsize=(10, 6))
    plt.hist(df_commune["average_note"], bins=bins, alpha=0.6, color='skyblue', edgecolor='black',
             label='Histogramme des notes')

    plt.title(f'Distribution de la note moyenne pour la commune de {nom_commune} (filtrage de 2021)')
    plt.xlabel("Note moyenne")
    plt.ylabel("Nombre de réponses")
    plt.legend()
    plt.grid(True)
    plt.savefig(f'{save_fold}/histo_avg_notes_{nom_commune}.png')
    plt.close()