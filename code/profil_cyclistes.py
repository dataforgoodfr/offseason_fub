import pandas as pd
from lecture_ecriture_donnees import preview_file, make_dir
from utils import get_commune_name_from_insee, get_insee_code_from_commune_name
import matplotlib.pyplot as plt
from local_paths import your_local_save_fold
import numpy as np
import textwrap

"""code pour analyser les questions liées au profil du cycliste. La fonction principale est profile_charecteristics"""

questions_id = [f'q{i}' for i in range(36, 49)]

profil_questions = ["Dans quel(s) but(s) utilisez-vous le vélo ?",
                    "A vélo, vous diriez que vous êtes...",
                    "Au cours de vos déplacements à vélo, avez-vous été victime d'une de ces situations ces douze derniers mois ? ",
                    "Est-ce que l'une de ces situations vous a amené à faire un dépôt de plainte/une main courante ?",
                    "Votre vélo habituel est un…",
                    "Pouvez-vous stationner facilement un vélo chez vous ou à proximité de chez vous ?",
                    "Avez-vous été victime d'un vol de vélo ces deux dernières années ? ",
                    "Avez-vous le permis de conduire ?",
                    "Avez-vous une voiture ou un deux-roues motorisé ?",
                    "Avez-vous un abonnement de transports en commun ?",
                    "Êtes-vous adhérent·e d'une association d'usagers du vélo (réseau FUB) ?",
                    "Quel est votre genre ?",
                    "Quelle est votre tranche d'âge ?"]


possible_answers = [{1:"Pour aller au travail", 2:"Pour aller à l'école", 3:"Pour mes déplacements utilitaires (achats, voir de la famille/amis, démarches administratives...)",
                     4:"Pour accompagner mes enfants", 5:"Pour le tourisme / balade", 6:"Pour faire du sport"},
                    {i:i for i in range(1,7)},
                    {1:"Violence verbale (insultes, intimidations) alors que vous circuliez sur un aménagement cyclable",
                     2:"Violence verbale alors que vous circuliez sur un espace partagé avec les véhicules motorisés",
                     3:"Violence physique (contact avec un véhicule ou mouvement du véhicule vers vous, coups ou tentative de coups) alors que vous circuliez sur un aménagement cyclable",
                     4:"Violence physique alors que vous circuliez sur un espace partagé avec les véhicules motorisés",
                     5:"Refus de priorité et queue de poisson du conducteur ",
                     6:"Vitesse et proximité dangereuses du conducteur ",
                     'nan':"Pas de situation identifiée"},
                    {1:"Oui", 2:"Non", 3:"La police ou la justice a donné suite à la plainte"},
                    {1:"Vélo personnel", 2:"Vélo à assistance électrique (VAE) personnel", 3:"Vélo en libre-service (Vélib', Vélo'v, V'Lille...)",
                     4:"Vélo en location longue-durée", 5:"Vélo adapté (vélo à enjambement bas, tricycle, quadricycle, etc.)"},
                    {1:"Oui, dans l'espace public", 2:"Oui, dans l'espace public avec une offre sécurisée proposée par la collectivité",
                     3:"Oui, dans mon lieu de résidence dans un espace accessible en rez-de-chaussée",
                     4:"Oui, dans mon lieu de résidence dans une cave ou un local nécessitant l'utilisation d'escalier ou d'ascenseur",
                     5:"Non", 6:"Ne se prononce pas"},
                    {1:"Oui", 2:"Non"},
                    {1:"Oui", 2:"Non"},
                    {1:"Oui", 2:"Non"},
                    {1:"Urbain (bus, tram, métro)", 2:"Inter-Urbain (TER, car)", 3:"Les deux", 4:"Non"},
                    {1:"Oui", 2:"Non"},
                    {1:"Hommes", 2:"Femmes"},
                    {1:"Moins de 11 ans", 2:"11-14 ans", 3:"15-18 ans", 4:"19-24 ans", 5:"25-34 ans", 6:"35-44 ans",
                     7:"45-54 ans", 8:"55-64 ans", 9:"65-75 ans", 10:"Plus de 75 ans"}]

def count_answers(sub_df):
    """Compte le nombre d'occurences de chacune des valeures des éléments de sub_df.
    ENTREES :
        - sub_df : pd.DataFrame. Data Frame de int ou de sting. Ses éléments contienent soit directement les références au réponses possibles
            (ex : [1 4 3 2 1 4]), soit lorsqu'il peut y avoir plusieurs réponses possibles, plusieurs références par cases, séparées par
             des virgules (ex ['1,4,3' '1' '2,3' '4 1'])
    SORTIES :
        - indices (np.array) : array contenant les références aux réponses possibles
        - counts (np.array de la même taille que indices) : nombre de contributions ayant répondu à chacune des réponses
    """

    answers = sub_df.astype(str)
    split_answers = np.concatenate(answers.str.split(',').values)
    indices, counts = np.unique(split_answers, return_counts=True)
    #indices = [int(float(idx)) for idx in indices]
    return indices, counts

def profile_characteristics_sub_df(sub_df, profile_df, possible_answers, row_name):
    indices, counts = count_answers(sub_df)
    profile_df.loc[row_name, "Nombre total de réponses"] = len(sub_df)
    for c in range(len(counts)):
        answer_idx = int(float(indices[c])) if indices[c] != "nan" else "nan"
        if answer_idx in possible_answers.keys():
            answer_label = possible_answers[answer_idx]
            nb_answers = counts[c]
            profile_df.loc[row_name, answer_label] = nb_answers / len(sub_df)

def profile_charecteristics(df, question_id, possible_answers, split_question_id, split_question_answers,
                            save_fold, title, ylabel="Nombre de réponses, en pourcentage du total", nb_answer_on_bar=False):
    """Sauvegarde un diagrammme en bar (et le tableau csv des valeures associées) qui précise la proportion de personnes
    ayant répondu pour chacune des réponses possibles (spécifiée dans possible_answers) à la question référencée par question_id.
    Les résultats sont séparées en fonction d'un critère de séparation (par exemple genre, catégorie du baromètre, ville ...) référencé
    par split_question_id.
    Le fichier csv sauvegardé contient N+1 colonne et M+1 lignes avec N = len(possible_answers) et M = len(split_question_answers).
    Les colonnes sont les différentes réponses possibles + une colonne associée au nombre total de réponses.
    Les lignes sont les différentes catégories de la spération + une colonne associé au décompe global des réponses (sans séparation)
    ENTREES :
        - df : data frame pandas contenant les résultats du questionnaire (si possible après nettoyage)
        - question_id (str) : référence de la colonne associées à la question que l'on souhaite analyser. df[question_id] est une colonne
            qui doit contenir les réponses à la question. La pluspart du temps les réponses sont encodées par des nombre entiers.
            Pour certaines questions, il est possible qu'une personne choissise plusieurs réponses. Dans ce cas les réponses possibles
            sont séparées par le symbole ','. (ex : '1,3').
        - possible_answers (dict) : dictionnaire dont les clés sont les identifiants des réponses (qui doivent être identiques à celles
         du data frame) et les valeures des string représentant les intitulés des réponses
         - split_question_id (str): identifiant de la question (i.e. colonne) par rapport à laquelle on souhaite séparer le data frame
                    (on peut vouloir séparé par genre, catégorie du baromètre etc..)
        - split_question_answers (dict) : dictionnaire dont les clés sont les identifiants des réponses de la variable de
            séparation et les valeures les intitulés associés
        - save_fold : chemin du dossier (en local) ou l'on souhaite sauvegardé les histogramme et fichiers csv associés au décompte
        - title : titre du graphique (qui est également utilisé dans le nom de sauvegarde)
        - nb_answers_on_bar (bool) : si True, on affiche le nombre de réponses total sur les bars du diagramme
    """

    profile_df = pd.DataFrame(columns=list(possible_answers.values()) + ["Nombre total de réponses"])
    for key in list(split_question_answers.keys()):
        sub_df = df.loc[df[split_question_id]==key, question_id]
        profile_characteristics_sub_df(sub_df, profile_df, possible_answers, split_question_answers[key])

    profile_characteristics_sub_df(df[question_id], profile_df, possible_answers, "Global")
    profile_df.to_csv(f'{save_fold}/{title}.csv')

    x = np.arange(len(possible_answers.values()))
    fig, ax = plt.subplots(figsize=(20, 12))
    s = 0.8
    width = s/len(split_question_answers.keys())
    for i,key in enumerate(list(split_question_answers.keys())):
        proportion_array = np.array(profile_df.loc[split_question_answers[key]].iloc[:-1].values)
        absolute_values_array = proportion_array * profile_df.loc[split_question_answers[key], "Nombre total de réponses"]
        bars = ax.bar(x+width*(i+0.5)-s/2, proportion_array, width, label=split_question_answers[key])
        if nb_answer_on_bar:
            for i,bar in enumerate(bars):
                height = bar.get_height()
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    height,
                    f'{int(absolute_values_array[i])}',
                    ha='center',
                    va='bottom',
                    fontsize=15
                )


    wrap_ylabel = '\n'.join(textwrap.wrap(ylabel, width=50))
    ax.set_ylabel(wrap_ylabel, fontsize=20)
    ax.set_title(title, fontsize=40)
    ax.set_xticks(x)
    wrapped_labels = ['\n'.join(textwrap.wrap(label, width=20)) for label in possible_answers.values()]
    ax.set_xticklabels(wrapped_labels)
    ax.legend(fontsize=20)
    ax.tick_params(axis='y', labelsize=20)
    ax.tick_params(axis='x', labelsize=15)
    plt.tight_layout()
    plt.savefig(f'{save_fold}/{title}.png')
    plt.close()


if __name__ == '__main__':
    insee_refs = preview_file(key="data/converted/2025/brut/220128_BV_Communes_catégories.csv", csv_sep=",", nrows=None)
    filtered_data_key = "data/converted/2025/nettoyee/250604_Export_Reponses_Final_Result_Nettoyee.csv"
    df = preview_file(filtered_data_key, nrows=None)
    save_fold = f"{your_local_save_fold}/barometre_profile_new_method"
    make_dir(save_fold)

    # tracé des graphes associés aux violenecs en séparant par genre
    q_idx = 2
    s_idx = -2  # séparation par genre
    title = 'Violences à vélo par genre'
    question_id, poss_answers = questions_id[q_idx], possible_answers[q_idx]
    split_question_id, split_question_answers = questions_id[s_idx], possible_answers[s_idx]
    profile_charecteristics(df, question_id, poss_answers, split_question_id, split_question_answers, save_fold, title, nb_answer_on_bar=True)

    # tracé des graphes associés aux violenecs en séparant par tranche d'âge
    s_idx = -1 # séparation par tranche d'âge
    title = "Violences à vélo par tranche d'âge"
    split_question_id, split_question_answers = questions_id[s_idx], possible_answers[s_idx]
    profile_charecteristics(df, question_id, poss_answers, split_question_id, split_question_answers, save_fold, title)

    # tracé des graphes associés aux violenecs des 10 plus grandes villes de France
    split_question_id = "insee"
    communes = ["Paris", "Marseille", "Lyon", "Toulouse", "Nice", "Nantes", "Montpellier", "Strasbourg", "Bordeaux", "Lille"]
    split_question_answers = {get_insee_code_from_commune_name(nom_commune, insee_refs)[0]:nom_commune for nom_commune in communes}
    title = "Violences à vélo, 10 plus grandes villes de France"
    profile_charecteristics(df, question_id, poss_answers, split_question_id, split_question_answers, save_fold, title)

    # tracé des graphes associés aux violences en séparant par catégorie du baromètre
    #get_categorie_from_insee = (lambda insee : get_commune_name_from_insee(insee, insee_refs)[1])
    df_categorie = df.copy()
    split_question_id = "Catégorie de commune"


    different_insees = df["insee"].unique()
    categories = {insee:get_commune_name_from_insee(insee, insee_refs)[1] for insee in different_insees} # dictionniare qui associe un code insee à la catégorie associée
    df_categorie[split_question_id] = df_categorie["insee"].apply(lambda insee:categories[insee]) # ajoute une colonne avec la catégorie du baromètre
                                                                                                    # pour ensuite pouvoir séparer selon cette variable.
    cat_labels = ['grandes villes', 'villes moyennes', 'communes de banlieue', 'petites villes', 'bourgs et villages']
    split_question_answers = {c:c for c in cat_labels}
    title = "Violences à vélo, par catégories du baromètre"
    profile_charecteristics(df_categorie, question_id, poss_answers, split_question_id, split_question_answers, save_fold, title)


    # tracé des graphes associés aux plaintes déposées
    question_id = "q39"
    df_plaintes = df.copy()
    df_plaintes = df_plaintes.dropna(subset=[question_id])
    df_plaintes[question_id] = df_plaintes[question_id].astype(str)

    def f(x):
        if x == 1:
            return ",3"
        else:
            return ""
    plaintes = df_plaintes["q62"].apply(f) # petite bidouille pour fusionner les réponses des questions q39 et q62 en une seule colonne (et donc pouvoir appliquer la même fonction que pour les autres cas)
    df_plaintes[question_id] = df_plaintes[question_id] + plaintes
    df_plaintes[question_id].to_csv(f'{your_local_save_fold}/test_df_pl.csv')
    poss_answers = {1:"Une plainte a été déposée", 3:"La police a donné suite à la plainte"}
    title = "Violence à vélo, dépôts de plaintes"
    split_question_id, split_question_answers = questions_id[-2], possible_answers[-2]

    # 
    profile_charecteristics(df_plaintes, question_id, poss_answers, split_question_id, split_question_answers, save_fold, title,
                            ylabel="Nombre de réponses, en pourcentage du nombre de personnes ayant déclaré avoir subi des violences", nb_answer_on_bar=True)


    nb_total = len(df)
    nb_violences_declarees = len(df_plaintes)
    nb_plaintes_deposees = len(df["q62"].dropna())
    X = [nb_total, nb_violences_declarees, nb_violences_declarees]
    df_nb_rep = pd.DataFrame(columns=["Nombre total de réponses", "Nombre de réponses q39 (violences déclarées)", "Nombre de réponses q62 (plainte déposée)"])
    df_nb_rep.loc[0] = [nb_total, nb_violences_declarees, nb_plaintes_deposees]
    df_nb_rep.to_csv(f'{save_fold}/nb_reponses_q38_39_62.csv')

