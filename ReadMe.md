# Mise en situation – Mission Data Engineer


## Arborescence du projet

```
Mise_en_situation---REF-DATA/
│
├── I. Traitement de données en Python/
│   ├── Inputs/
│   │   ├── patients.csv.gz
│   │   └── consultations.csv.zip
│   │
│   ├── outputs/
│   │   ├── patients.parquet
│   │   ├── consultations.parquet
│   │   └── resultat_proportion.parquet
│   │
│   ├── logs/
│   │   └── processing.log
│   │
│   ├── notebook/
│   │   └── exploration.ipynb
│   │
│   └── src/
│       └── process_data.py
│
├── II. SQL et Systèmes Relationnels/
│   └── exo.sql
│
├── requirements.txt
├── README.md
└── .gitignore
```

---

## Partie I – Traitement de données en Python

### Environnement virtuel

Un **environnement virtuel Python** a été utilisé pour ce projet.

Objectifs :

* isoler les dépendances du projet,
* garantir la reproductibilité,
* éviter les conflits avec d’autres projets Python.

Les bibliothèques nécessaires à l’exécution du pipeline sont listées dans le fichier :

```
requirements.txt
```

Ce fichier contient uniquement les dépendances indispensables à l’exécution :

* `pandas`
* `numpy`
* `pyarrow`

Les bibliothèques standards Python (ex. `logging`, `pathlib`) ne sont pas listées car elles sont incluses nativement avec Python.

---

### Organisation du traitement Python

Le travail est volontairement séparé en **deux niveaux** :

#### 1. Notebook – Exploration des données

* Fichier : `notebook/exploration.ipynb`
* Rôle :

  * exploration initiale des données,
  * compréhension des structures,
  * vérification des hypothèses,
  * tests intermédiaires.

Le notebook n’a pas vocation à être un pipeline final, mais un **support d’analyse et de réflexion**.

---

#### 2. Script Python – Pipeline de traitement

* Fichier : `src/process_data.py`
* Rôle :

  * réponse finale à la problématique,
  * pipeline reproductible et automatisé,
  * exécution hors notebook,
  * génération de logs et de fichiers de sortie.

Le script est structuré en **4 étapes principales** :

1. Lecture des données
2. Nettoyage et typage explicite
3. Jointure et analyse
4. Sauvegarde des résultats

---

### Organisation des données

* **Inputs**
  Dossier `Inputs/`
  Contient les fichiers sources compressés (`.csv.gz`, `.csv.zip`).

* **Outputs**
  Dossier `outputs/`
  Contient les résultats du traitement au format **Parquet**.

* **Logs**
  Dossier `logs/`
  Le fichier `processing.log` trace toutes les étapes du pipeline (démarrage, étapes, fin, volumes traités).

---

## Partie II – SQL et Systèmes Relationnels

### Contenu

* Fichier : `II. SQL et Systèmes Relationnels/exo.sql`
* La requête permet d’analyser :

  * l’évolution annuelle de la fréquentation féminine,
  * par spécialité de praticien,
  * en respectant les règles de gestion (consultations non annulées, exhaustivité, jointures appropriées).

La requête est commentée et structurée pour en faciliter la lecture.

---

