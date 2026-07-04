# 🎬 Spark MD4 — Pipeline ETL MovieLens

Pipeline de données Apache Spark (ETL, architecture médaillon Bronze/Silver/Gold) construit sur le jeu de données **MovieLens** (`ml-latest-small`), dans le cadre du projet Jour 4.

*Équipe : MEKDAM Ghiles, AOUIMEUR Ouissem, CHABA Ramdane.*

---

## 📦 Contenu du dépôt

```
.
├── starter-code/       # Squelette de départ (SparkSession, pipeline à trous)
│   ├── pipeline.py
│   ├── spark_session.py
│   ├── requirements.txt
│   └── README.md
├── data/
│   ├── download.sh     # Script de téléchargement du jeu de données
│   ├── output/         # Sorties Silver/Gold (Parquet, analyses)
│   └── analyses/
├── images/              # Captures d'écran Spark UI + DAGs
├── test_pipeline.py      # Tests unitaires du pipeline
└── rapport_project.md    # Rapport détaillé du projet (analyses, résultats, optimisations)
```

## 🚀 Démarrage rapide

### 1. Prérequis
- Python 3.9+
- Java 17 ou 21 (requis par Spark 4) — vérifier avec `java -version`

### 2. Installation

```bash
python -m venv .venv
source .venv/bin/activate        # Windows : .venv\Scripts\activate
pip install -r starter-code/requirements.txt
```

### 3. Récupérer les données

```bash
bash data/download.sh
```

### 4. Lancer le pipeline

```bash
python starter-code/pipeline.py
```

### 5. Suivre l'exécution dans la Spark UI

Pendant l'exécution : [http://localhost:4040](http://localhost:4040)

### 6. Lancer les tests

```bash
python -m unittest test_pipeline.py
```

## 🏗️ Architecture

Pipeline en 3 couches (Bronze → Silver → Gold) : ingestion avec schémas explicites, nettoyage/dédoublonnement, enrichissement (extraction de l'année, conversion de timestamp), puis trois analyses métier (top films, statistiques par genre, top 5 par genre via window function).

Optimisations mises en œuvre : mise en cache des tables réutilisées, broadcast join sur la table `movies`, partitionnement Parquet par `rating` avec vérification du partition pruning.

## 📊 Résultats et détails

Le détail complet des analyses, des résultats, des mesures de performance et des captures d'écran Spark UI se trouve dans **[`rapport_project.md`](./rapport_project.md)**.

## 🧪 Tests

Le pipeline est validé par un module de tests unitaires (`test_pipeline.py`) couvrant les valeurs nulles, les doublons, les notes hors-limites et les échecs de regex sur l'année.
