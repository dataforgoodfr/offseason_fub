import os.path

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from scipy.stats import norm, binom
from lecture_ecriture_donnees import preview_file, write_csv_on_s3


def filter_one_commune_2025_method(df_commune, questions_to_average, avg_note_att_name="average_note", alpha=10**-3):
    """Applique la méthodologie de nettoyage à un tableau pandas associé à une commune partiuclière (df_commune).
    La méthodologie est la suivante :
    1) calcul d'une moyenne et d'un écart type ajustés à l'aide de la méthode compute_adjusted_mean_std
    2) Comptage du nombre d'éléments dans la queue inférieure (valeures de notes moyennes inférieures à adjusted_mean - 2*adjusted_std)
        et dans la queue supérieure (valeures supérieures à adjusted_mean + 2*adjusted_std)
    3) Si le nombre d'élements dans la queue inférieure N_low ou dans la queue supérieure N_upp est largement supérieur au nombre d'éléments
        auquel on aurait pu s'attendre si la distribution était gaussienne, alors les queues sont supprimées (i.e. seule la partie centrale
        de la distribution est conservée).

    Plus précisément un formalisme de test d'hypothèse est utilisé, avec pour hypothèse nulle H0 : la distribution des notes moyennes
    est gaussienne. Si la distribution est gaussienne, alors on sait que la probabilité qu'un échantillon soit
    inférieur à moyenne - 2*ecart-type vaut p = norm.cdf(-2) (=0.026) . La variable aléatoire Y qui compte le nombre d'échantillons
    inférieurs à moyenne - 2*ecart-type suit alors une loi binomiale B(N_sample, p). On calcule ensuite la valeur k telle P(Y>k) = alpha.
    Ensuite, si N_low > k, on en déduit que la probabilité d'obtenir un compte N_low aussi élevé ou plus élevé en supposant que la distibution est gaussienne
    est inférieur à alpha, l'hypothèse nulle H0 "la distribution est gaussienne" est alors rejetée.

    Limite de la méthode : le rejet de l'hypothèse "la distribution gaussienne est rejetée" n'est en réalité pas suffisant pour conclure qu'il y a eu
    une fraude. Il peut y avoir d'autres raisons (par exemple, une partie de la ville est bien desservie en pistes cyclables et l'autre non).
    En utilisant le formalisme de test d'hypothèse décrit plus haut, il s'avérait que l'algorithme rejetait l'hypothèse H0 bien trop ouvent même
    avec des valeurs de alpha extremement faibles. C'est pourquoi, on choisit de filter les queues inférieures et supérieures seulement
    lorsque N_low ou N_upp sont supérieurs à 2*k (au lieu de k).

    ENTREES :
        - df_commune (pd.DataFrame) : sous-ensemble des réponses associé à une commune particulière
        - questions_to_average (list). Liste contenant l'ensemble des noms de colonnes associées aux questions pour lesquelles
                        une note de 1 à 6 était demandée.
        - avg_note_att_name (str) : un attribut "note moyenne" est ajouté au tableau, avg_note_att_name est le nom de cet attribut
        - alpha : seuil utilisé pour le test d'hypothèse
    SORTIES :
        - filtered_data (pd.DataFrame) : jeu de données nettoyé (égale au jeu de données d'entrées s'il n'a pas été invalidé que la distribution
                                        est gaussienne, égale aux données de la partie centrale de la distribution sinon)
        - filter (bool): boolean qui indique si les queues ont été supprimées ou non
        - adjusted_mean, adjusted_std (float) : moyennes et variances ajustées
    """


    df_commune[avg_note_att_name] = df_commune[questions_to_average].mean(axis=1)
    adjusted_mean, adjusted_std = compute_adjusted_mean_std(df_commune[avg_note_att_name])

    upper_queue = df_commune[df_commune[avg_note_att_name] >= adjusted_mean + 2*adjusted_std]
    lower_queue = df_commune[df_commune[avg_note_att_name] <= adjusted_mean - 2*adjusted_std]
    central_values = df_commune[(df_commune[avg_note_att_name] >= adjusted_mean - 2*adjusted_std)
                                & (df_commune[avg_note_att_name] <= adjusted_mean + 2*adjusted_std)]
    N_sample = len(df_commune)
    p = norm.cdf(-2)  # probabilité qu'un echantillon aléatoire d'une distribution gaussienne soit inférieur à mu - 2*std
    k = int(binom.ppf(1 - alpha, N_sample, p)) # k est tel que P(Y>k) = alpha avec Y ~ B(N_sample, p)
    print('N sample', N_sample)
    print('k', k)
    print('len upper queue', len(upper_queue))
    print('len lower queue', len(lower_queue))
    filter = (len(upper_queue) >= 2*k or len(lower_queue) >= 2*k)
    if filter:
        filtered_data = central_values.copy()
    else:
        filtered_data = df_commune.copy()
    return filtered_data, filter, adjusted_mean, adjusted_std




def filter_one_commune_2019_method(df_commune, questions_to_average, email_id, avg_note_att_name="average_note"):
    """recodage de la methodo de 2019 (pas utilisée ici)"""
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
    adjusted_mean, adjusted_std = compute_adjusted_mean_std(filtered[avg_note_att_name])
    # suppression des réponses au dela de moyenne +- 2.5 écart-type
    filtered = filtered[(filtered[avg_note_att_name] >= adjusted_mean - 2.5 * adjusted_std)
                        & (filtered[avg_note_att_name] <= adjusted_mean + 2.5 * adjusted_std)]
    # supperssion des emails doublons (si une personne répond 2 fois)
    # print('len filtered 3', len(filtered))
    # filtered = filtered.drop_duplicates(subset=[email_id], keep='first') # not working because a lot of NaN values for email (detected as identical)
    print('Nombre de réponses après filtrage', len(filtered))
    return filtered

def filter_data_set(df, questions_to_average, commune_id, save_key, insee_refs, histo_save_fold,
                    communes_to_save, nb_contribution_min=10, avg_note_att_name="average_note"):
    """Applique la méthodologie de nettoyage des données à l'ensemble des communes, écris les données nettoyées sur le S3 et
    sauvegarde en local les histogrames des notes moyennes (avant et après filtrage) de certaines communes, à savoir les communes spéccifiées par la variable
    commune_to_save, et les communes pour lesquels une fraude potentiel a été detectée. La méthodologie de nettoyage est effectuée par la
    fonction filter_one_commune_2025_method.
    ENTREES:
        - df (pd.DataFrame), Shape (Nombre de réponses, Nombre de questions).
            Data Frame pandas contenant l'ensemble des réponses au questionnaire de la FUB. Shape
        - questions_to_average (list). Liste contenant l'ensemble des noms de colonnes associées aux questions pour lesquelles
                        une note de 1 à 6 était demandée.
        - commune_id (str). Nom de la colonne associée à la question demandant la commune à évaluer
        - save_key (str). Chemin de sauvegarde des données nettoyées sur le S3
        - insee_refs (pd.DataFrame). Tableau pandas associant les codes INSEE au nom de commune et autres caractéristiques de la commune
        - histo_save_fold : chemin (en local) de sauvegarde des histogrammes des notes moyennes
        - commune_to_save (list de str). Liste contenant les noms de communes pour lesquelles l'on souhaite sauvegarder les histogrammes des notes moyennes.
         En plus des communes de la liste seont sauverardée les histogrammes des communes pour lesquels il y a une fraude potentielle
        - nb_contribution_min (int) : toutes les communes ayant moins de contributions (avant nettoyage) que cette valeur sont supprimées
        - avg_note_att_name (str) : un attribut "note moyenne" est ajouté au tableau, avg_note_att_name est le nom de cet attribut
    SORTIES:
        - all_filtered_data (pd.DataFrame). Le tableau pandas contenant les données netoyées
    """

    if not os.path.exists(histo_save_fold):
        os.makedirs(histo_save_fold)
    insee_codes = df[commune_id].unique()
    all_filtered_data = []
    df = df.dropna(subset=questions_to_average)
    for insee_code in insee_codes:
        nom_commune, _, _ = get_commune_name_from_insee(insee_code, insee_refs)
        df_commune = df[df[commune_id] == insee_code].copy()
        if len(df_commune) >= nb_contribution_min:
            # moyennage de l'ensemble des critères d'évaluation
            filtered, filter, adjusted_mean, adjusted_std = filter_one_commune_2025_method(df_commune, questions_to_average, avg_note_att_name)
            if nom_commune in communes_to_save or filter:
                plot_histo(df_commune[avg_note_att_name],1,6,0.2, adjusted_mean, adjusted_std,
                           f"Distribution de la note moyenne pour la commune {nom_commune} (avant filtrage)",
                           f'{histo_save_fold}/histo_avg_notes_{nom_commune}_avant_filtrage.png')

                plot_histo(filtered[avg_note_att_name], 1, 6, 0.2, adjusted_mean, adjusted_std,
                           f"Distribution de la note moyenne pour la commune {nom_commune} (après filtrage)",
                           f'{histo_save_fold}/histo_avg_notes_{nom_commune}_après_filtrage.png')
            all_filtered_data.append(filtered)
    all_filtered_data = pd.concat(all_filtered_data, ignore_index=True)
    # all_filtered_data.to_csv("/home/thibaut/filtered.csv", index=False)
    write_csv_on_s3(all_filtered_data, save_key)
    return all_filtered_data

def get_commune_name_from_insee(insee_code, insee_refs):
    """
    Determine le nom de commune associé à un code insee
    ENTREES
        insee_code (str) : code insee d'une commune
        insee_refs (pd.DataFrame) : tableau pandas associant les codes INSEE au nom de commune et autres caractéristiques de la commune
            (attributs du tableau pandas : "INSEE", "TYP_COM", "STATUT_2017", "DEP", "REG", "POPULATION", "Catégorie Baromètre" "EPCI")
    SORTIES
        nom_commune : le nom de la commune associée au code insee d'entrée
        categorie : categorie de commune associé (ec : grande villes, villes moyennes, bourgs et villages etc ...)
        not_found (bool) : vaut True ssi le numéro INSEE n'a pas été trouvé dans le tableau. Dans ce cas, la commune est considérée
            comme égale à insee_code.

    """
    nom_commune = insee_refs.loc[
        insee_refs["INSEE"] == insee_code, "Commune"]  # récupère le nom de la commune associée au code INSEE
    categorie = insee_refs.loc[insee_refs["INSEE"] == insee_code, "Catégorie Baromètre"]
    not_found = (len(nom_commune) == 0)
    if not_found:
        print(f"Le numéro INSEE {insee_code} n'a pas été trouvé dans le tableau des communes")
        nom_commune = insee_code
        categorie = 'Not found'
    else:
        nom_commune = nom_commune.item()
        categorie = categorie.item()
    return nom_commune, categorie, not_found

def compute_adjusted_mean_std(df):
    """calcul la moyenne ajustée et l'écart-type ajusté d'un data frame pandas, définis comme les moyennes et écart-types
     du data frame auquel on a supprimé les valeurs extrêmes. Ces dernières sont définies comme les valeurs inférieur à 1.5 ou supérieur à 5.5 ou
    au dela de la moyenne initiale + ou - 2 l'écart type initial
    ENTREE :
        df (pd.DataFrame) : un data frame pandas unidimensionnel
    SORTIE :
        adjusted_mean (float) : la moyenne ajustée calculé sur df après avoir supprimé les valeures extrêmes
        adjusted_std (float) : l'écart-type ajusté
    """

    mean_avg_notes, std_avg_notes = df.mean(), np.nan_to_num(df.std())  # std peut etre egal à 0 sil y a un seul élément dans le tableau
    # calcul des deuxièmes moyennes et ecart type après avoir supprimé les réponses au dela de moyenne +- 2 écarts types
    df2 = df[(df >= mean_avg_notes - 2 * std_avg_notes) & (df <= mean_avg_notes + 2 * std_avg_notes) & (df>1.5) & (df<5.5)]
    adjusted_mean, adjusted_std = df2.mean(), np.nan_to_num(df2.std())
    return adjusted_mean, adjusted_std


def plot_histo(values, start, stop, step, adjusted_mean, adjusted_std, title, save_path):
    """trace l'histogramme des valeures de values et superpose cet histogramme avec une distribution gaussienne de
     moyenne adjusted_mean et d'ecart type adjusted_std"""
    # Données pour la gaussienne
    x = np.linspace(start, stop, 1000)
    bin_width = step
    n = len(values)
    y = norm.pdf(x, adjusted_mean, adjusted_std) * n * bin_width  # Ajustement à l'échelle des effectifs

    # Création de l'histogramme
    bins = np.arange(start, stop + step, step)
    plt.figure(figsize=(10, 6))
    plt.hist(values, bins=bins, alpha=0.6, color='skyblue', edgecolor='black',
             label='Histogramme des notes')

    # Tracé de la courbe gaussienne
    plt.plot(x, y, 'r-', label='Distribution gaussienne')

    # Tracé des lignes verticales pointillées
    plt.axvline(adjusted_mean, color='black', linestyle='--', linewidth=1.5, label='Moyenne ajustée')
    plt.axvline(adjusted_mean - 2 * adjusted_std, color='gray', linestyle='--', linewidth=1.2, label='-2 écarts-types')
    plt.axvline(adjusted_mean + 2 * adjusted_std, color='gray', linestyle='--', linewidth=1.2, label='+2 écarts-types')

    # Légendes et affichage
    plt.title(title)
    plt.xlabel("Note moyenne")
    plt.ylabel("Nombre de réponses")
    plt.legend()
    plt.grid(True)
    plt.savefig(save_path)
    plt.close()




if __name__ == '__main__':
    data_2025 = False # = True si on souhaite nettoyer les données de 2025, =False si on souhaite nettoyer les données de 2021

    data = preview_file(key="data/converted/2025/brut/reponses-2025-04-29_Result 1.csv", nrows=None, csv_sep=",") \
        if data_2025 else preview_file(key="data/converted/2021/brut/reponses-2021-12-01-08-00-00.csv", nrows=None)

    print('loaded')
    # print('col insee', insee_refs.columns)

    email_id = "email" if data_2025 else "q56"
    emails = data[email_id]
    commune_id = "insee" if data_2025 else "q01" # insee = communes pour les données de 2025, "q01" = communes pour les données de 2021

    questions_to_average = [f"q{i}" for i in range(7,34)] \
        if data_2025 else [f"q{i}" for i in range(14, 41)]
    # questions associées à l'ensemble des critères d'évalutations pour l'année 2025

    save_key = "data/converted/2025/nettoyee/reponses-2025-04-29-filtered.csv" if data_2025 else \
                "data/reproduced/2021/reponses-2021-12-01-08-00-00_filtered_2025_method.csv"
    histogram_save_fold = "../histograms/histograms_2025" if data_2025 else "../histograms/histograms_2021"
    insee_refs = preview_file(key="data/converted/2025/brut/220128_BV_Communes_catégories.csv", csv_sep=",", nrows=None)

    communes_to_save = ["Dainville", "Strasbourg", "Illkirch-Graffenstaden", "Lyon", "Paris", "Marseille", "Vesoul"]
    #communes_to_save = ["Villeneuve-de-la-Raho", "Notre-Dame-de-Monts", "Vieux-Boucau-les-Bains"]
    all_filtered_data = filter_data_set(data, questions_to_average, commune_id, save_key, insee_refs,
                                        histogram_save_fold, communes_to_save)
    print('filtered data shape', all_filtered_data.shape)
    #all_filtered_data.to_csv("/home/thibaut/filtered.csv", index=False)




