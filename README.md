## Description du projet 

Ce projet vise à traiter les données du Baromètre Vélo. Réalisé tous les deux ans par la FUB depuis 2017, il invite les cyclistes et non cyclistes à s’exprimer sur leur ressenti afin d’améliorer les conditions de déplacement à vélo. Pour plus détail sur le contexte général, voir [ici](https://outline.services.dataforgood.fr/doc/presentation-du-projet-Eq1Lc9yIyM)

## Stockage des données 

Les données utilisées dans le projet (réponses bruts et nettoyées au questionnaire, données annexes, résultats d'analyse etc...) sont sauvegardé sur un bucket S3. Des fonctions pour lire et écrire des données sur ce bucket sont disponibles dans le fichier *lecture_ecriture_donnees.py*. Pour pouvoir lire ces données sur le bucket S3, il faut spécifier les identifiants associés. Ces derniers vous seront communiqués si vous souhaitez contribuer au projet. Vous devrez alors créer un fichier .env dans lequel il faudra spécifier les identifiants de la façon suivante : 
    
    AWS_ACCESS_KEY_ID= *clé d'accès transmise*
    AWS_SECRET_ACCESS_KEY= *clé secrête transmise*

