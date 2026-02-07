"""
Script de traitement des données
Mise en situation – Traitement de données en Python

Étapes :
1. Lecture
2. Nettoyage & Typage
3. Jointure & Analyse
4. Sauvegarde
"""

from pathlib import Path
import logging
import pandas as pd
import numpy as np


def setup_logging(log_path: Path) -> None:
    """Configure le logging (fichier + console)."""
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def main() -> None:
    # ============================================================
    # Définition des chemins (robuste même si on lance depuis /src)
    # ============================================================
    project_root = Path(__file__).resolve().parents[1]
    inputs_dir = project_root / "inputs"
    outputs_dir = project_root / "outputs"
    logs_dir = project_root / "logs"

    setup_logging(logs_dir / "processing.log")

    logging.info("===== DÉBUT DU TRAITEMENT =====")

    # ============================================================
    # 1. LECTURE DES DONNÉES
    # ============================================================
    logging.info("Étape 1 - Lecture des fichiers CSV")

    patients_path = inputs_dir / "patients.csv.gz"
    consultations_path = inputs_dir / "consultations.csv.zip"

    # Lecture du fichier patients (gzip)
    df_patients = pd.read_csv(
    patients_path,
    compression="gzip",
    encoding="latin-1"
    )

    # Lecture du fichier consultations (zip)
    df_consultations = pd.read_csv(
        consultations_path,
        compression="zip",
        encoding="latin-1"
    )

    logging.info(
        "Lecture OK | patients=%s lignes | consultations=%s lignes",
        len(df_patients),
        len(df_consultations),
    )

    # ============================================================
    # 2. NETTOYAGE & TYPAGE
    # ============================================================
    logging.info("Étape 2 - Nettoyage et typage explicite")

    logging.info("Colonnes patients: %s", list(df_patients.columns))
    logging.info("Colonnes consultations: %s", list(df_consultations.columns))

    # --- Patients

    # Nettoyage des valeurs non-signifiantes

    df_patients["patient_id"] = (
        df_patients["patient_id"]
        .str.strip()         # supprime les espaces
        .replace("", np.nan) # remplace les chaînes vides par NaN
    )

    df_patients["birth_date"] = df_patients["birth_date"].replace(
        ["not_a_date", "N/ A"], np.nan
    )   

    df_patients["gender"] = df_patients["gender"].replace("unknown", np.nan)

    # Typage explicite des colonnes

    # Identifiant patient : chaîne de caractères
    df_patients["patient_id"] = df_patients["patient_id"].astype("string")
    # Date de naissance : datetime
    df_patients["birth_date"] = pd.to_datetime(
        df_patients["birth_date"]
    )
    nb_birth_invalid = df_patients["birth_date"].isna().sum()
    logging.info("Patients exclus (birth_date invalide): %s", nb_birth_invalid)
    # Genre : variable catégorielle
    df_patients["gender"] = df_patients["gender"].astype("category")

    # ============================================================
    # Justification des types – table patients
    # ============================================================

    # patient_id -> string
    # Le champ `patient_id` est un identifiant technique.
    # Il n’est pas destiné à des calculs numériques mais à des comparaisons
    # et des jointures. Le type `string` permet de conserver l’intégrité
    # de l’identifiant (y compris d’éventuels zéros ou caractères
    # alphanumériques) et garantit une jointure fiable avec la table
    # `consultations`.

    # birth_date -> datetime
    # La date de naissance est convertie en `datetime` afin de permettre
    # des traitements temporels cohérents (comparaisons de dates,
    # calculs d’âge, contrôles de validité). Le typage explicite permet
    # également de détecter et neutraliser les valeurs invalides lors
    # du parsing.

    # gender -> category
    # La variable `gender` possède un nombre limité de modalités connues.
    # Le type `category` est donc adapté, car il améliore la lisibilité
    # sémantique de la variable et permet une optimisation mémoire par
    # rapport à un type texte classique.


    # --- Consultations
    
    # Nettoyage des valeurs non-signifiantes

    df_consultations["consultation_id"] = (
        df_consultations["consultation_id"]
        .str.strip()         # supprime les espaces
        .replace("", np.nan)
    )

    df_consultations["date_consultation"] = df_consultations["date_consultation"].replace(
        ["not_a_date", "N/ A"], np.nan
    )

    df_consultations["diagnostic"] = df_consultations["diagnostic"].replace("nnull", np.nan)

    # Typage

    # Identifiant de consultation : chaîne de caractères
    df_consultations["consultation_id"] = df_consultations["consultation_id"].astype("string")
    # Identifiant patient : chaîne de caractères (clé de jointure)
    df_consultations["patient_id"] = df_consultations["patient_id"].astype("string")
    # Date de consultation : datetime
    df_consultations["date_consultation"] = pd.to_datetime(
        df_consultations["date_consultation"],
        format="%d/%m/%Y"
    )
    nb_date_invalid = df_consultations["date_consultation"].isna().sum()
    logging.info("Consultations exclues (date invalide): %s", nb_date_invalid)
    # Diagnostic : variable catégorielle
    df_consultations["diagnostic"] = df_consultations["diagnostic"].astype("category")

    # ============================================================
    # Justification des types – table consultations
    # ============================================================

    # consultation_id -> string
    # Le champ `consultation_id` est un identifiant unique de consultation.
    # Il est typé en `string` car il s’agit d’un identifiant métier,
    # non numérique, utilisé uniquement pour l’identification et non
    # pour des opérations arithmétiques.

    # patient_id -> string
    # Le champ `patient_id` est utilisé comme clé de jointure avec la table
    # `patients`. Il est typé en `string` pour assurer la cohérence de type
    # entre les deux tables et éviter tout problème de jointure lié à des
    # conversions implicites.

    # date_consultation -> datetime
    # La date de consultation est convertie en `datetime` afin de permettre
    # l’extraction d’informations temporelles (mois, année) nécessaires
    # à l’analyse. Ce typage explicite garantit également une interprétation
    # correcte du format de date et l’exclusion des valeurs invalides.

    # diagnostic -> category
    # Le champ `diagnostic` correspond à un ensemble restreint de statuts
    # ou de libellés. Le type `category` est approprié pour représenter
    # ce type de variable qualitative et facilite les analyses descriptives
    # ultérieures.

    logging.info("Nettoyage et typage terminés")

    # ============================================================
    # 3. JOINTURE & ANALYSE
    # ============================================================
    logging.info("Étape 3 - Jointure et analyse")

    # Jointure consultations ↔ patients
    df_joined = df_consultations.merge(
        df_patients[["patient_id"]],
        on="patient_id",
        how="left",
        indicator=True,
    )

    # Indicateur de validité
    df_joined["patient_valide"] = df_joined["_merge"] == "both"

    # Extraire le mois (sans convertir en string)
    df_joined["mois_consultation"] = df_joined["date_consultation"].dt.to_period("M")

    # Supprimer les consultations sans mois valide
    df_joined = df_joined.dropna(subset=["mois_consultation"])

    # Conversion finale en string (pour affichage / export)
    df_joined["mois_consultation"] = df_joined["mois_consultation"].astype(str)

    # Calculer la proportion par mois 
    resultat = (
        df_joined
        .groupby("mois_consultation")["patient_valide"]
        .mean()
        .reset_index(name="proportion_patient_id_valide")
    )

    logging.info("Analyse terminée | %s lignes produites", len(resultat))

    # ============================================================
    # 4. SAUVEGARDE DES RÉSULTATS
    # ============================================================
    logging.info("Étape 4 - Sauvegarde au format Parquet")

    outputs_dir.mkdir(parents=True, exist_ok=True)

    df_patients.to_parquet(outputs_dir / "patients.parquet", index=False)
    df_consultations.to_parquet(outputs_dir / "consultations.parquet", index=False)
    resultat.to_parquet(outputs_dir / "resultat_proportion.parquet", index=False)

    logging.info("Sauvegarde terminée")
    logging.info("===== FIN DU TRAITEMENT (SUCCESS) =====")


if __name__ == "__main__":
    main()
