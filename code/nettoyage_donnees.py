from local_paths import your_local_save_fold
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from scipy.stats import norm, binom
from lecture_ecriture_donnees import preview_file, write_csv_on_s3, make_dir
from utils import get_commune_name_from_insee


def filter_one_commune_2025_method(df_commune, commune_name, communes_to_filter=[], communes_not_to_filter=[],
                                   avg_note_att_name="average_note", alpha=8*10**-4, beta=2):
    """Applique la méthodologie de nettoyage à un tableau pandas associé à une commune partiuclière (df_commune).
    La méthodologie est la suivante :
    1) calcul d'une moyenne et d'un écart type ajustés à l'aide de la méthode compute_adjusted_mean_std
    2) Comptage du nombre d'éléments dans la queue inférieure (valeures de notes moyennes inférieures à adjusted_mean - 2*adjusted_std)
        et dans la queue supérieure (valeures supérieures à adjusted_mean + 2*adjusted_std)
    3) Si le nombre d'élements dans la queue inférieure N_low ou dans la queue supérieure N_upp est largement supérieur au nombre d'éléments
        auquel on aurait pu s'attendre si la distribution était gaussienne, alors la queue dont le nombre d'élément est trop important
        est partiellement supprimée (on supprime les éléments de cette queue de sorte à garder autant d'élement que dans l'autre queue,
        de sorte à ne pas désiquilibrer la distribution de l'autre côté)

    Plus précisément un formalisme de test d'hypothèse est utilisé, avec pour hypothèse nulle H0 : la distribution des notes moyennes
    est gaussienne. Si la distribution est gaussienne, alors on sait que la probabilité qu'un échantillon soit
    inférieur à moyenne - 2*ecart-type vaut p = norm.cdf(-2) (=0.026) . La variable aléatoire Y qui compte le nombre d'échantillons
    inférieurs à moyenne - 2*ecart-type suit alors une loi binomiale B(N_sample, p). On calcule ensuite la valeur k telle P(Y>k) = alpha.
    Ensuite, si N_low > k, on en déduit que la probabilité d'obtenir un compte N_low aussi élevé ou plus élevé en supposant que la distibution est gaussienne
    est inférieur à alpha, l'hypothèse nulle H0 "la distribution est gaussienne" est alors rejetée.

    Limite de la méthode : le rejet de l'hypothèse "la distribution gaussienne est rejetée" n'est en réalité pas suffisant pour conclure qu'il y a eu
    une fraude. Il peut y avoir d'autres raisons (par exemple, une partie de la ville est bien desservie en pistes cyclables et l'autre non).
    En utilisant le formalisme de test d'hypothèse décrit plus haut, il s'avérait que l'algorithme rejetait l'hypothèse H0 bien trop souvent même
    avec des valeurs de alpha extremement faibles. C'est pourquoi, on choisit de filter les queues inférieures et supérieures seulement
    lorsque N_low ou N_upp sont supérieurs à beta*k (au lieu de k).

    ENTREES :
        - df_commune (pd.DataFrame) : sous-ensemble des réponses associé à une commune particulière
        -commune_name (str) : nom de la commune
        - communes_to_filter (list de str) : communes pour lesquelles on souhaite supprimer les valeures extrême quoi qu'il arrive
        - communes_not_to_filter (list de str) : communes pour lesquelles on ne souhaite pas supprimer les valeurs extrêmes quoi qu'il arrive
        - avg_note_att_name (str) : un attribut "note moyenne" est ajouté au tableau, avg_note_att_name est le nom de cet attribut
        - alpha : seuil utilisé pour le test d'hypothèse
    SORTIES :
        - filtered_data (pd.DataFrame) : jeu de données nettoyé (égale au jeu de données d'entrées s'il n'a pas été invalidé que la distribution
                                        est gaussienne, égale aux données de la partie centrale de la distribution sinon)
        - filter (bool): boolean qui indique si les queues ont été supprimées ou non
        - adjusted_mean, adjusted_std (float) : moyennes et variances ajustées
    """
    adjusted_mean, adjusted_std = compute_adjusted_mean_std(df_commune[avg_note_att_name], commune_name)

    upper_queue = df_commune[df_commune[avg_note_att_name] >= adjusted_mean + 2*adjusted_std]
    lower_queue = df_commune[df_commune[avg_note_att_name] <= adjusted_mean - 2*adjusted_std]
    central_values = df_commune[(df_commune[avg_note_att_name] >= adjusted_mean - 2*adjusted_std)
                                & (df_commune[avg_note_att_name] <= adjusted_mean + 2*adjusted_std)]

    N_sample = len(df_commune)
    p = norm.cdf(-2)  # probabilité qu'un echantillon aléatoire d'une distribution gaussienne soit inférieur à mu - 2*std
    k = int(binom.ppf(1 - alpha, N_sample, p)) # k est tel que P(Y>k) = alpha avec Y ~ B(N_sample, p)
    if commune_name in communes_not_to_filter:
        filtered_data = df_commune.copy()
        filter = False
    elif (len(upper_queue) >= beta*k and len(lower_queue) >= beta*k) or commune_name in communes_to_filter:
        filtered_data = central_values.copy()
        filter = True
    elif len(upper_queue) >= beta*k and len(lower_queue) <= beta*k:
        nb_elt_to_supress = len(upper_queue) - len(lower_queue)
        limit_val = df_commune[avg_note_att_name].nlargest(nb_elt_to_supress).iloc[-1]
        filtered_data = df_commune[df_commune[avg_note_att_name]<limit_val].copy()
        filter = True
    elif len(upper_queue) <= beta*k and len(lower_queue) >= beta *k:
        #print('len lower', len(lower_queue))
        #print('len upper', len(upper_queue))

        nb_elt_to_suppress = len(lower_queue) - len(upper_queue)
        #print('nb elt to suppress', nb_elt_to_suppress)
        #print('n smallest', df_commune[avg_note_att_name].nsmallest(nb_elt_to_suppress))
        limit_val = df_commune[avg_note_att_name].nsmallest(nb_elt_to_suppress).iloc[-1]

        filtered_data = df_commune[df_commune[avg_note_att_name]>limit_val].copy()
        filter = True
    else: #len(upper_queue) <= 2*k and len(lower_queue) <= 2 *k:
        filtered_data = df_commune.copy()
        filter = False
    largest_queue = upper_queue if len(upper_queue) > len(lower_queue) else lower_queue
    return filtered_data, filter, adjusted_mean, adjusted_std, largest_queue

"""
def filter_ip(df_commune):
    df_ip_doublon_view = create_view_with_identical_ip(df_commune, ip_id, commentaire_id, email_id, avg_note_att_name, x=2)
"""

def filter_one_commune_ip(df_commune, ip_id, commentaire_id, email_id, avg_note_att_name, x=2, y=5):
    """créer une vue du tableau d'entrée ou seul sont conservées les lignes pour lesquels l'adresse ip associée à la
    ligne apparait au moins x fois dans le tableau. Supprime ensuite les contributions pour lesquelles il y a au moins y
    adresses ip identiques et la moyenne des notes associées à ces ip identiques est supérieures à 5 ou inférieure à 2"""
    ip_counts = df_commune[ip_id].value_counts()
    ip_doublon = ip_counts[ip_counts>=x].index
    df_ip_doublon = df_commune[df_commune[ip_id].isin(ip_doublon)]
    df_ip_doublon_view = df_ip_doublon[[ip_id, avg_note_att_name, email_id, commentaire_id]]
    df_ip_doublon_view = df_ip_doublon_view.sort_values(by=ip_id)
    agg = df_ip_doublon_view.groupby([ip_id]).agg({avg_note_att_name:'mean', ip_id:'count'})
    fraudulous_ip = agg[((agg[avg_note_att_name] >= 5) | (agg[avg_note_att_name] <= 2)) & (agg[ip_id] >= y)].index
    df_commune = df_commune[~df_commune[ip_id].isin(fraudulous_ip)]
    filter = (len(fraudulous_ip)>0)
    return df_commune, df_ip_doublon_view, fraudulous_ip, filter


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

def filter_data_set(df, questions_to_average, commune_id, commentaire_id, ip_id, email_id, save_key, insee_refs, histo_save_fold,
                    communes_to_save, communes_to_filter=[], communes_not_to_filter=[], nb_contribution_min=[30,50], avg_note_att_name="average_note"):
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
        - commentaire_id (str). Nom de la colonne associé aux commentaires qualitatifs
        - save_key (str). Chemin de sauvegarde des données nettoyées sur le S3
        - insee_refs (pd.DataFrame). Tableau pandas associant les codes INSEE au nom de commune et autres caractéristiques de la commune
        - histo_save_fold : chemin (en local) de sauvegarde des histogrammes des notes moyennes. Le dossier est séparé en 3
            sous-dossier, dans le dossier "potential_fraud_detected" sont sauvegardés les communes pour lesquelles une fraude potentielle a été detecté.
            Dans le dossier "specified_communes" sont sauvegardées les communes pour lesquelles spécifiées dans la liste communes_to_save.
            Dans le dossier "identical_ip" sont sauvegardées des csv associées à des contributions avec adresse ip identiques.
        - commune_to_save (list de str). Liste contenant les noms de communes pour lesquelles l'on souhaite sauvegarder les histogrammes des notes moyennes.
         En plus des communes de la liste seont sauverardée les histogrammes des communes pour lesquels il y a une fraude potentielle
        - communes_to_filter (list de str) : communes pour lesquelles on souhaite supprimer les valeures extrême quoi qu'il arrive
        - communes_not_to_filter (list de str) : communes pour lesquelles on ne souhaite pas supprimer les valeurs extrêmes quoi qu'il arrive
        - nb_contribution_min (list of  2 int) : toutes les communes de moins de 5000 habitants  ayant moins de contributions
                    que nb_contribution_min[0] sont supprimées. Toutes les communes de plus de 5000 habitants ayant moins de
                    contributions que nb_contribution_min[1] sont supprimées
        - avg_note_att_name (str) : un attribut "note moyenne" est ajouté au tableau, avg_note_att_name est le nom de cet attribut
    SORTIES:
        - all_filtered_data (pd.DataFrame). Le tableau pandas contenant les données netoyées
    """
    make_dir(histo_save_fold)
    #df = df.dropna(subset=questions_to_average)
    insee_codes = df[commune_id].unique()
    all_filtered_data = []
    print('nombre de communes avec au moins 1 contribution', len(insee_codes))
    potential_fraudulous_communes = pd.DataFrame(columns=["Nom commune", "Commune éliminée après nettoyage", "Nombre de contributions supprimées",
                                                          "Nombre de contributions avant nettoyage",
                                                          "Nombre de contributions après nettoyage",
                                                          "filtrage ip", "filtrage distribution"])
    communes_with_identical_ip = []
    df = df.dropna(subset=questions_to_average)
    for insee_code in insee_codes:
        nom_commune, _, population, _ = get_commune_name_from_insee(insee_code, insee_refs)
        df_commune = df[df[commune_id] == insee_code].copy()
        n_min = nb_contribution_min[0] if population <= 5000 else nb_contribution_min[1]
        if len(df_commune) >= n_min:
            df_commune[avg_note_att_name] = df_commune[questions_to_average].mean(axis=1)
            filtered, df_ip_doublon_view, fraudulous_ip, filter_ip = filter_one_commune_ip(df_commune, ip_id, commentaire_id, email_id, avg_note_att_name)

            # moyennage de l'ensemble des critères d'évaluation
            filtered, filter_distr, adjusted_mean, adjusted_std, largest_queue = filter_one_commune_2025_method(filtered,nom_commune,
                                                                                                                communes_to_filter, communes_not_to_filter, avg_note_att_name)
            filter = (filter_distr or filter_ip)
            if len(df_ip_doublon_view) > 0:
                save_fold_ip = f'{histo_save_fold}/identical_ip'
                make_dir(save_fold_ip)
                communes_with_identical_ip.append(nom_commune)
                df_ip_doublon_view.to_csv(f'{save_fold_ip}/identical_ip_{nom_commune}.csv')
                if filter_ip:
                    pd.DataFrame(fraudulous_ip).to_csv(f'{save_fold_ip}/fraudoulous_ip_{nom_commune}.csv')

            save_folds = np.array([f'{histo_save_fold}/specified_communes', f'{histo_save_fold}/potential_fraud_detected'])
            save_folds = save_folds[[nom_commune in communes_to_save, filter]]
            if filter:
                row = [nom_commune, len(filtered)<n_min, len(df_commune)-len(filtered), len(df_commune), len(filtered), filter_ip, filter_distr]
                potential_fraudulous_communes.loc[len(potential_fraudulous_communes)] = row
            for save_fold in save_folds:
                make_dir(save_fold)
                plot_histo(df_commune[avg_note_att_name],1,6,0.2, adjusted_mean, adjusted_std,
                           f"Distribution de la note moyenne pour la commune {nom_commune} (avant filtrage)",
                           f'{save_fold}/histo_avg_notes_{nom_commune}_avant_filtrage.png')

                plot_histo(filtered[avg_note_att_name], 1, 6, 0.2, adjusted_mean, adjusted_std,
                           f"Distribution de la note moyenne pour la commune {nom_commune} (après filtrage)",
                           f'{save_fold}/histo_avg_notes_{nom_commune}_après_filtrage.png')

                plot_histo_response_time(df_commune, f'{save_fold}/histo_time_response_{nom_commune}.png',
                                         largest_queue, nom_commune)
                commentaries = df_commune[[avg_note_att_name, ip_id, commentaire_id]].dropna(subset=[commentaire_id])
                commentaries.to_csv(f'{save_fold}/commentraires_qualitatifs_{nom_commune}.csv')
                if len(df_ip_doublon_view) > 0:
                    df_ip_doublon_view.to_csv(f'{save_fold}/identical_ip_{nom_commune}.csv')



            if len(filtered) >= n_min:
                all_filtered_data.append(filtered)
    print('Nombre de communes qualifiées', len(all_filtered_data))
    print('Nombre de communes potentiellement frauduleuse', len(potential_fraudulous_communes))
    print('Nombre de communes avec des ips identiques', len(communes_with_identical_ip))
    #print('Communes avec des ips identiques', communes_with_identical_ip)
    #print('Communes avec des ips identiques, qui n"ont pas été détectées frauduleuse',
          #[c for c in communes_with_identical_ip if c not in potential_fraudulous_communes])
    potential_fraudulous_communes = potential_fraudulous_communes.sort_values(by="Nombre de contributions supprimées", ascending=False)
    #potential_fraudulous_communes = potential_fraudulous_communes.sort_values(by="Nom commune",
                                 #                                             ascending=False)
    potential_fraudulous_communes.to_csv(f'{histo_save_fold}/_potentielles_fraudes.csv')
    all_filtered_data = pd.concat(all_filtered_data, ignore_index=True)
    # all_filtered_data.to_csv("/home/thibaut/filtered.csv", index=False)
    # write_csv_on_s3(all_filtered_data, save_key)
    return all_filtered_data


def compute_adjusted_mean_std(df, commune_name):
    """calcul la moyenne ajustée et l'écart-type ajusté d'un data frame pandas, définis comme les moyennes et écart-types
     du data frame auquel on a supprimé les valeurs extrêmes. Ces dernières sont définies comme les valeurs inférieur à 1.5 ou supérieur à 5.5 ou
    au dela de la moyenne initiale + ou - 2 l'écart type initial. S'il s'avère que adjusted_mean + 2*adjusted_std dépasse 5.5, adjusted_std
    est modifié tel que adjusted_mean + 2*adjusted_std = 5.5. En effet, pour de rares cas adjusted_mean + 2*adjusted_std dépassait 6, et donc
    la méthodologie de nettoyage qui compte le nombre de valeurs extremes ne fonctionnait pas (idem pour adjusted_mean - 2*adjusted_std
    inférieur à 1.5)
    ENTREE :
        df (pd.DataFrame) : un data frame pandas unidimensionnel
        commune_name (str) : le nom de la commune
    SORTIE :
        adjusted_mean (float) : la moyenne ajustée calculé sur df après avoir supprimé les valeures extrêmes
        adjusted_std (float) : l'écart-type ajusté
    """

    mean_avg_notes, std_avg_notes = df.mean(), np.nan_to_num(df.std())  # std peut etre egal à 0 sil y a un seul élément dans le tableau
    # calcul des deuxièmes moyennes et ecart type après avoir supprimé les réponses au dela de moyenne +- 2 écarts types
    df2 = df[(df >= mean_avg_notes - 2 * std_avg_notes) & (df <= mean_avg_notes + 2 * std_avg_notes) & (df>1.5) & (df<5.5)]
    adjusted_mean, adjusted_std = df2.mean(), np.nan_to_num(df2.std())
    if adjusted_mean + 2*adjusted_std >= 5.5:
        #print('adjusted std modifié 5.5', commune_name)
        adjusted_std = (5.5 - adjusted_mean)/2
    if adjusted_mean - 2*adjusted_std <= 1.5:
        #print('adjusted std modifié 1.5', commune_name)
        adjusted_std = (adjusted_mean - 1.5)/2
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


def plot_histo_response_time(df_commune, save_path, largest_queue, nom_commune, x=12):
    """trace l'histogramme des dates de remplissage du formulaire.
    ENTREES :
        df_commune (pd.DataFrame) : sous-ensemble du questionnaire
        save_path (str) : localisation de sauvegarde de l'histogramme
        x (int) : nombre d'heures dans un intervalle de l'histogramme"""
    # Conversion de la colonne 'date' en datetime
    dates = pd.to_datetime(df_commune["date"], errors='coerce')
    dates = dates.dropna()

    # Regrouper les dates par tranche de x heures
    bins = pd.date_range(start=dates.min().floor('h'),
                         end=dates.max().ceil('h'),
                         freq=f'{x}h')

    # Histogramme : découpe les dates selon les intervalles
    counts, _ = pd.cut(dates, bins=bins, right=False, retbins=True)
    hist = counts.value_counts().sort_index()

    dates_largest_queue = pd.to_datetime(largest_queue["date"], errors='coerce').dropna()
    counts_largest_queue, _ = pd.cut(dates_largest_queue, bins=bins, right=False, retbins=True)
    hist_largest_queue = counts_largest_queue.value_counts().sort_index()


    # Tracer l'histogramme
    plt.figure(figsize=(12, 6))
    plt.bar([interval.left for interval in hist.index], hist.values, width=pd.Timedelta(hours=x))
    plt.bar([interval.left for interval in hist_largest_queue.index], hist_largest_queue.values, width=pd.Timedelta(hours=x), color='orange')
    plt.gcf().autofmt_xdate()
    plt.xlabel(f"Date (pas de {x} heures)")
    plt.ylabel("Nombre d'événements")
    plt.title(f"Histogramme des dates de réponse {nom_commune} ({x}h)")
    plt.tight_layout()
    plt.savefig(save_path)
    plt.close()


if __name__ == '__main__':
    data_2025 = True # = True si on souhaite nettoyer les données de 2025, =False si on souhaite nettoyer les données de 2021

    data = preview_file(key="data/converted/2025/brut/250604_Export_Reponses_Brut_Final_Result 1.csv", nrows=None, csv_sep=",") \
        if data_2025 else preview_file(key="data/converted/2021/brut/reponses-2021-12-01-08-00-00.csv", nrows=None)

    print('loaded')
    # print('col insee', insee_refs.columns)

    email_id = "email" if data_2025 else "q56"
    emails = data[email_id]
    commune_id = "insee" if data_2025 else "q01" # insee = communes pour les données de 2025, "q01" = communes pour les données de 2021
    commentaire_id = "q35" if data_2025 else "q42"
    ip_id = "ip"
    questions_to_average = [f"q{i}" for i in range(7,34)] \
        if data_2025 else [f"q{i}" for i in range(14, 41)]
    # questions associées à l'ensemble des critères d'évalutations pour l'année 2025

    save_key = "data/converted/2025/nettoyee/250604_Export_Reponses_Final_Result_Nettoyee.csv" if data_2025 else \
                "data/reproduced/2021/reponses-2021-12-01-08-00-00_filtered_2025_method.csv"
    histogram_save_fold = f"{your_local_save_fold}/histograms_good_data/histograms_2025_potential_frauds" if data_2025 else f"{your_local_save_fold}/histograms/histograms_2021"
    insee_refs = preview_file(key="data/converted/2025/brut/220128_BV_Communes_catégories.csv", csv_sep=",", nrows=None)


    communes_not_to_filter = ["Grenoble", "Paris", "Lyon", "Marseille", "Gap", "Lunéville", "Neuilly-Plaisance", "Pibrac",
                                "Ploemeur", "Villemomble", "Voiron"]

    communes_to_filter = []
    communes_to_save = ["Montpellier", "Groix", "Gâvres", "Port-Louis", "Riantec", "Guidel", "Hennebont", "Cabourg",
    "Dives-sur-Mer", "Bayeux", "Toulon", "Ollioules", "Le Lavandou", "Sainte-Maxime", "La Croix-Valmer",
    "Bormes-les-Mimosas", "Cavalaire-sur-Mer", "Ramatuelle", "Saint-Cyr-sur-Mer", "La Londe-les-Maures",
    "Gassin", "Grimaud", "Carqueiranne", "Les Baux-de-Provence", "Mas-Blanc-des-Alpilles", "Sénas", "Lambesc",
    "Lançon-Provence", "Éguilles", "Saint-Paul-lès-Durance", "Berre-l'Étang", "Mimet", "La Penne-sur-Huveaune",
    "Saint-Chamas", "Le Rove", "Saint-Victoret", "Gémenos", "Saint-Mitre-les-Remparts", "Gignac-la-Nerthe",
    "Châteauneuf-les-Martigues", "Cassis", "Port-de-Bouc", "Fos-sur-Mer", "Belcodène", "Aurons",
    "Maussane-les-Alpilles", "Peyrolles-en-Provence", "Jouques", "Vernègues", "La Barben"]



    all_filtered_data = filter_data_set(data, questions_to_average, commune_id, commentaire_id, ip_id, email_id,
                                        save_key, insee_refs, histogram_save_fold, communes_to_save, communes_to_filter,
                                        communes_not_to_filter)


    #communes_to_save = ["Bourg-en-Bresse", "Gujan-Mestras", "Cherbourg-en-Cotentin", "La Rochelle", "Chambéry",
                        #"Valserhône", "Cambrai", "Carcassonne", "Le Tampon", "Béziers"] ## meilleures et moins bonnes communes de la catégorie villes moyennes


    #communes_to_save = ["Grenoble", "Strasbourg", "Lyon", "Rennes", "Tour", "Annecy", "Nantes",
                        #"Marseille", "Saint-Étienne", "Perpignan", "Limoges", "Aix-en-Provence", "Toulon", "Nîmes"] ## meilleures et moins bonnes communes de la catégorie grandes villes

    #communes_to_save = ["Saint-Aubin-de-Médoc", "Le Teich", "Le Bourget-du-Lac", "Lieusaint", "Dainville",
                        #"Ars-sur-Moselle", "Les Pennes-Mirabeau", "Ventabren", "Aubagne", "Arnage"] ## meilleures et moins bonnes communes de la catégorie communes de banlieues

    #communes_to_save = ["Val-de-Reuil", "Andernos-les-Bains", "Bretignolles-sur-Mer", "Acigné", "Mèze",
                        #"Esbly", "Gérardmer", "Caussade", "Le Pian-Médoc", "Champagnole"]  # meilleures et moins bonnes communes de la catégorie petites villes

    
   
    #communes_to_save = ["Jullouville", "Vieux-Boucau-les-Bains", "Notre-Dame-de-Monts", "Le Trait", "Soulac-sur-Mer", "Le Touquet-Paris-Plage",
                        #"Drémil-Lafage", "Saint-Uze", "Muizon", "Veigny-Foncenex", "Chadrac"] # meilleures et moins bonnes communes de la catégorie bourg et villages

    #communes_to_save = ["L'Île-d'Yeu", "Île-de-Bréhat", "Ouessant"]



    #all_filtered_data.to_csv("/home/thibaut/filtered.csv", index=False)




