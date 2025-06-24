import os

def make_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

your_local_save_fold = "/home/thibaut/Documents/fub_results" # chemin local pour la sauvegarde des histogrammes, tableaux de notes, pour une visualisation plus rapide
                                                                # que lorsque les données sont sauvegadéees sur le s3