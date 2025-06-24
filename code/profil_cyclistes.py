import pandas as pd
from lecture_ecriture_donnees import preview_file
from utils import get_commune_name_from_insee
from local_paths import your_local_save_fold


"""code pour analyser les questions liées au profil du cycliste, non encore fonctionnel"""

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


possible_answers = [["Pour les trajets domicile-travail ou études", "Pour faire des achats, des démarches personnelles",
                     "Pour faire du sport", "Pour se promener"],
                    range(1,7),
                    ["Violence verbale (insultes, intimidations) alors que vous circuliez sur un aménagement cyclable",
                     "Violence verbale alors que vous circuliez sur un espace partagé avec les véhicules motorisés",
                     "Violence physique (contact avec un véhicule ou mouvement du véhicule vers vous, coups ou tentative de coups) alors que vous circuliez sur un aménagement cyclable",
                     "Violence physique alors que vous circuliez sur un espace partagé avec les véhicules motorisés",
                     "Refus de priorité et queue de poisson du conducteur ", "Vitesse et proximité dangereuses du conducteur ", "Pas de situation identifiée"],
                    ["Oui", "Non", "La police ou la justice a donné suite à la plaine"],
                    ["Vélo personnel", "Vélo à assistance électrique (VAE) personnel", "Vélo en libre-service (Vélib', Vélo'v, V'Lille...)",
                     "Vélo en location longue-durée", "Vélo adapté (vélo à enjambement bas, tricycle, quadricycle, etc.)"],
                    ["Oui, dans l'espace public", "Oui, dans l'espace public avec une offre sécurisée proposée par la collectivité",
                     "Oui, dans mon lieu de résidence dans un espace accessible en rez-de-chaussée",
                     "Oui, dans mon lieu de résidence dans une cave ou un local nécessitant l'utilisation d'escalier ou d'ascenseur",
                     "Non", "Ne se prononce pas"],
                    ["Oui", "Non"],
                    ["Oui", "Non"],
                    ["Oui", "Non"],
                    ["Urbain (bus, tram, métro)", "Inter-Urbain (TER, car)", "Les deux", "Non"],
                    ["Oui", "Non"],
                    ["Féminin", "Masculin", "Ne se pronnonce pas"],
                    ["Moins de 11 ans", "11-14 ans", "15-18 ans", "18-24 ans", "25-34 ans", "35-44 ans",
                     "45-54 ans", "55-64 ans", "65-75 ans", "Plus de 75 ans", "Ne se prononce pas"]]



def profile_characteristics_statistics(df, insee_refs, questions_id, profil_questions, possible_answers, save_fold,
                                       commune_id="insee", commune_type_id="Catégorie Baromètre"):

    df_en_tete = [[profil_questions[i] for i in range(len(profil_questions)) for _ in range(len(possible_answers[i]))],
              [possible_answers[i][j] for i in range(len(possible_answers)) for j in range(len(possible_answers[i]))]]

    columns = pd.MultiIndex.from_arrays(df_en_tete)
    insee_codes = df[commune_id].unique()
    types_of_communes = insee_refs[commune_type_id].unique()
    profile_dfs = {categorie:pd.DataFrame(columns=columns) for categorie in types_of_communes}
    not_found_insee_codes = []
    for insee_code in insee_codes:
        df_commune = df[df[commune_id] == insee_code]
        nom_commune, categorie, _, not_found = get_commune_name_from_insee(insee_code, insee_refs)
        print('nom commune', nom_commune)
        if not_found:
            not_found_insee_codes.append(insee_code)
        for i in range(len(questions_id)):
            answers = df_commune[questions_id[i]]
            counts = answers.value_counts()
            print('counts', counts)
            print('poss answer', possible_answers[i])
            for c in range(len(counts)):
                print('idw', counts.index[c])
                answer_idx = int(counts.index[c]) - 1
                print('answ', answer_idx)
                answer_label = possible_answers[i][answer_idx]
                nb_answers = counts.values[c]
                profile_dfs[categorie].loc[nom_commune, (profil_questions[i], answer_label)] = nb_answers
    for categorie in profile_dfs.keys():
        profile_dfs[categorie] = profile_dfs[categorie].sort_index()
        profile_dfs[categorie].to_csv(f'{save_fold}/caractéristique_profiles_{categorie}.csv')
    return profile_dfs

if __name__ == '__main__':
    insee_refs = preview_file(key="data/converted/2025/brut/220128_BV_Communes_catégories.csv", csv_sep=",", nrows=None)
    filtered_data_key = "data/converted/2025/nettoyee/reponses-2025-04-29-filtered.csv"
    df = preview_file(filtered_data_key, nrows=None)
    commune_id = "insee"
    commune_type_id = "Catégorie Baromètre"
    save_fold = f"{your_local_save_fold}/barometre_profile"
    profile_characteristics_statistics(df, insee_refs, questions_id, profil_questions, possible_answers, save_fold, commune_id, commune_type_id)
