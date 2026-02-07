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
    df_patients = df_patients.dropna(subset=["patient_id"])
    df_patients["patient_id"] = df_patients["patient_id"].astype("string")
    df_patients["birth_date"] = pd.to_datetime(
        df_patients["birth_date"], errors="coerce"
    )
    df_patients["gender"] = df_patients["gender"].astype("category")

    # --- Consultations
    df_consultations = df_consultations.dropna(
        subset=["consultation_id", "patient_id"]
    )
    df_consultations["consultation_id"] = df_consultations["consultation_id"].astype("string")
    df_consultations["patient_id"] = df_consultations["patient_id"].astype("string")
    df_consultations["date_consultation"] = pd.to_datetime(
    df_consultations["date_consultation"],
    format="%d/%m/%Y",
    errors="coerce"
    )
    df_consultations["diagnostic"] = df_consultations["diagnostic"].astype("category")

    logging.info("Nettoyage et typage terminés")

    # ============================================================
    # 3. JOINTURE & ANALYSE
    # ============================================================
    logging.info("Étape 3 - Jointure et analyse")

    df_joined = df_consultations.merge(
        df_patients[["patient_id"]],
        on="patient_id",
        how="left",
        indicator=True,
    )

    df_joined["patient_valide"] = df_joined["_merge"] == "both"

    df_joined["mois_consultation"] = (
        df_joined["date_consultation"].dt.to_period("M").astype(str)
    )

    resultat = (
        df_joined.groupby("mois_consultation")["patient_valide"]
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
