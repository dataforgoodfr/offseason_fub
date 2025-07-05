

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
    population = insee_refs.loc[insee_refs["INSEE"] == insee_code, "Population"]
    not_found = (len(nom_commune) == 0)
    if not_found:
        print(f"Le numéro INSEE {insee_code} n'a pas été trouvé dans le tableau des communes")
        nom_commune = insee_code
        categorie = 'Not found'
        population = 4000 # Sera tratité comme une petite commune (30 contributions minimum)
    else:
        nom_commune = nom_commune.item()
        categorie = categorie.item()
        population = population.item()
    return nom_commune, categorie, population, not_found


def get_insee_code_from_commune_name(nom_commune, insee_refs):
    insee_code = insee_refs.loc[insee_refs["Commune"] == nom_commune, "INSEE"]
    categorie = insee_refs.loc[insee_refs["Commune"] == nom_commune, "Catégorie Baromètre"]
    population = insee_refs.loc[insee_refs["Commune"] == nom_commune, "Population"]
    not_found = (len(insee_code) == 0)
    if not_found:
        print(f"La commune {nom_commune} n'a pas été trouvé dans le tableau des communes")
        insee_code = nom_commune
        categorie = 'Not found'
        population = 4000 # Sera tratité comme une petite commune (30 contributions minimum)
    else:
        insee_code = insee_code.item()
        categorie = categorie.item()
        population = population.item()
    return insee_code, categorie, population, not_found