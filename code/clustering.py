from lecture_ecriture_donnees import preview_file
import prince
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from local_paths import your_local_save_fold


#data = preview_file(key="data/converted/2025/nettoyee/processed/250604_Export_Reponses_Final_Result_Nettoyee_Processed.csv", csv_sep=";", nrows=None)
"""Ce code explore la possibilité de réaliser du clustering sur les données du baromètre, c'est-à-dire regrouper les contributions en 
groupes d'éléments statistiquements simialires entre eux (et suffisament différents des éléments des autres groupes). 

2 approches sont considérés : 
- l'une prenant en compte uniquement les variables catégorielles du questionnaire de la section "profil cyclistes" du questionnaire (c'est-à-dire les questions 36 à 48)
- l'autre prenant en compte ces mêmes variables avec en plus l'ensemble des variables quantitatives (correspondant aux notes attribuées aux différents critères) (q36 à 48 + q7 à
    q32).

Dans les deux cas, il n'est pas possible d'utiliser un algorithme de clustering classique (comme kmeans ou dbscan par exemple) 
 sur les données brutes car ces derniers utilisent une distance euclidienne pour comparer les données entre elles, distance qui n'est pas adaptée aux
 données catégorielles (pour lesquelles on privilégie une distance du Xhi2). 
 
L'approche retenue dans ce code est de transformer l'espace dans lequel on effectue le clustering de sorte à ce que le calcul d'une distance euclidienne 
dans ce nouvel espace soit équivalent à une distance du Xhi2 dans l'espace original. L'algorithme utilisé pour changer d'espace est l'algorithme ACM lorsqu'on
considère uniquement des variables catégorielles et FAMD lorsqu'on considère le mélange de données catégorielles et quantitatives.

Après avoir effectué ce changement d'espace, nous constatons qu'il n'y a qu'un seul cluster qui se dégage. Ceci a été vérifié visuellement pour des espaces latents à dimensions 
2 et 3, et pour des espaces a dimensions supérieurs, nous traçons la distribution des distances 2 à 2 entre l'ensemble des points, et l'on constate
que cette dernière a une allure de distribution gaussienne unimodale, ce qui confirme que même dans des espaces à dimensions supérieures, un seul cluster se dégage. 
(si on avait plusieurs cluster , on s'attendrait à observer que la distribution des distances 2 à 2 soient multimodale). 

Conclusion : Effectuer du clustering sur ces données aboutirait à un résultat où les distances moyennes entre éléments de mêmes cluster 
ne soiéent pas largement inférieures aux distances entre les centroïd des clusters. Effectuer du clustering sur ces données n'est donc 
pas pertinent.

D'autres analyses exploratoires sont effectuées. Par exemple , on aimerait pouvoir répondre à la question : est-ce que des 
contributions proches les unes des autres dans l'espace latent ont des notes moyennes associées proches ? On se rend compte que ce n'est pas le cas en visualisant
l'epsace latent avec les points labélisés par les notes moyennes.

On analyse également la différence de profils entre les cyclistes et non cyclistes en calculant le barycentre des contributions 
dans l'espace latent. On se rend compte que la différence entre les deux est extremement faible (en utilisant les 5 variables communes aux cyclistes et non cyclistes)"""

def plot_latent_space(coords, labels, legend='', nb_dim=2, color_barycenter='green'):
    if nb_dim==3:
        fig = plt.figure()
        ax = fig.add_subplot(111, projection='3d')  # active la projection 3D
        if labels is None:
            labels = [1] * len(coords)
        ax.scatter(coords.iloc[:, 0], coords.iloc[:, 1], coords.iloc[:, 2], c=labels)
    else:
        barycenter = coords.mean(axis=0)
        plt.scatter(coords.iloc[:, 0], coords.iloc[:, 1], c=labels, label=legend)
        plt.scatter(barycenter[0], barycenter[1], c=color_barycenter, s=100)

def plot_latent_space_size_prop_to_count(coords, labels, legend=''):
    counts = coords.value_counts().reset_index(name='count')
    plt.scatter(counts.loc[:, 0], counts.loc[:, 1], s=counts.loc[:,'count'], c=labels)

def fit_reduction_dim_model(model, df):
    model.fit(df)
    row_coords = model.row_coordinates(df)
    expanded = np.expand_dims(row_coords, axis=1)
    repeated_array = np.repeat(expanded, len(row_coords), axis=1)
    transposed_array = np.transpose(repeated_array, (1,0,2))
    two_by_two_distances = np.mean((repeated_array - transposed_array)**2, axis=2)
    plt.hist(two_by_two_distances.flatten(), bins=np.arange(0,np.max(two_by_two_distances)//2,0.1))
    plt.show()
    return row_coords

def one_hot_encode_categorial_with_several_responses(df, column_names):
    """one hote encoding des questiosn pour lesquelles plusieurs réponses sont possibles (car ce type de
    one hot ebcoding n'est pas traité par les fonctions de réduction de dimensionalité FAMF et ACM)
    ENTREES :
    df : pd.DataFrame qui contenant les données
    column_names : identifiants des colonnes que l'on souhaite one hote encoder. Les éleéments de ces colonnes sont de la forme "1,3"
    (des entiers séparées par des virgules)
    SORTIES
    df_copy : data frame one hote encodé
    """
    df_copy = df.copy()
    for c in column_names:
        sub_df = df_copy[c]
        answers = sub_df.astype(str)
        diff_answers = np.unique(np.concatenate(answers.str.split(',').values))
        for answer in diff_answers:
            col_name = f'{c} {answer}'
            def f(x):
                return answer in x.split(',')
            df_copy[col_name] = answers.apply(f).astype('category')
    df_copy.drop(columns=column_names)
    return df_copy

if __name__ == '__main__':
    #df = preview_file(key="data/converted/2025/nettoyee/250604_Export_Reponses_Final_Result_Nettoyee.csv", csv_sep=";",
                      #nrows=None)
    df = pd.read_csv("bdd.csv")
    #df.to_csv(f'{your_local_save_fold}/data_nettoye.csv')

    print('columns3', df.columns)
    quantitative_variables = [f"q{i}" for i in range(6, 34)] + ['average_note']
    categorial_variables = ["q36","q37","q38"] + [f'q{i}' for i in range(40, 49)]
    variables_to_keep = quantitative_variables + categorial_variables
    nb_contr_to_keep = 5000
    df_used_for_clustering = df[variables_to_keep]
    contr_to_keep = np.random.randint(0, len(df_used_for_clustering), nb_contr_to_keep)
    df_used_for_clustering = df_used_for_clustering.iloc[contr_to_keep]
    print('shape df', df_used_for_clustering.shape)
    df_used_for_clustering[categorial_variables] = df_used_for_clustering[categorial_variables].astype('category')

    df_used_for_clustering_one_hot = one_hot_encode_categorial_with_several_responses(df_used_for_clustering, ['q36', 'q38'])
    print('col', df_used_for_clustering_one_hot.columns)

    famd = prince.FAMD(
        n_components=6,
        random_state=42
    )

    fit_reduction_dim_model(famd, df_used_for_clustering_one_hot)

    df_acm = df_used_for_clustering[categorial_variables].astype('category')
    df_acm_one_hot = one_hot_encode_categorial_with_several_responses(df_acm, ['q36', 'q38'])
    print('col', df_acm_one_hot.columns)
    mca = prince.MCA(
        n_components=6,
        n_iter=3,
        copy=True,
        check_input=True,
        engine='sklearn',
        random_state=42
    )
    columns = [f'q{i}' for i in range(40, 49)]
    row_coords = fit_reduction_dim_model(mca, df_acm_one_hot[columns])
    plot_latent_space(row_coords, labels=np.array(df_used_for_clustering['average_note']))
    plt.show()
    plot_latent_space_size_prop_to_count(row_coords, labels='y')
    plt.show()
    df_acm_2 = df_acm[['q43', 'q44', 'q45', 'q47', 'q48']].astype('category')
    mca_2 = prince.MCA(
        n_components=5,
        n_iter=3,
        copy=True,
        check_input=True,
        engine='sklearn',
        random_state=42
    )
    row_coords = fit_reduction_dim_model(mca_2, df_acm_2)

    print('df acm cyclistes', df_acm_2)


    key_non_cyclistes = "data/converted/2025/nettoyee/250604_Export_Reponses_Final_Result_Non_Cyclistes.csv"
    #df_non_cyclistes = preview_file(key_non_cyclistes)
    #df_non_cyclistes.to_csv('bdd_non_cyclistes.csv')
    df_non_cyclistes = pd.read_csv('bdd_non_cyclistes.csv')
    df_acm_non_cycl = df_non_cyclistes[['q53', 'q54', 'q55', 'q56', 'q57']].astype('category')
    df_acm_non_cycl = df_acm_non_cycl.dropna()
    contr_to_keep = np.random.randint(0, len(df_acm_non_cycl), nb_contr_to_keep)
    df_acm_non_cycl = df_acm_non_cycl.iloc[contr_to_keep]
    sub = df_acm_non_cycl.iloc[:5000,:]
    sub.to_csv('sub.csv')

    df_acm_non_cycl.columns = df_acm_2.columns
    for c in df_acm_non_cycl.columns:
        print(df_acm_non_cycl[c].unique())
        print(df_acm_2[c].unique())
    print('columns', df_acm_non_cycl.columns)
    row_coords_non = mca_2.row_coordinates(df_acm_non_cycl)

    print('rwo coords non', row_coords_non)
    plot_latent_space(row_coords_non, labels=['r'] * len(row_coords), legend='non cyclistes', color_barycenter='green')
    plot_latent_space(row_coords, labels=['y'] * len(row_coords), legend='cyclistes', color_barycenter='blue')
    plt.legend()
    plt.show()

    plot_latent_space_size_prop_to_count(row_coords_non, labels=['r'], legend='non cyclistes')
    plot_latent_space_size_prop_to_count(row_coords, labels=['y'], legend='cyclistes')
    plt.show()









