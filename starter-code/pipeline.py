"""Pipeline de données Spark complet pour l'analyse du jeu de données MovieLens (ratings et movies).

Rendu par le groupe de 3 personnes.
Ce pipeline implémente l'architecture Bronze -> Silver -> Gold.
"""

import sys
import time
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, LongType
)

from spark_session import get_spark

# Chemins d'accès aux données
MOVIES_BRUT = "data/datasets/ml-latest-small/movies.csv"
RATINGS_BRUT = "data/datasets/ml-latest-small/ratings.csv"
SORTIE_SILVER_MOVIES = "data/output/silver/movies"
SORTIE_SILVER_RATINGS = "data/output/silver/ratings"
SORTIE_GOLD = "data/output/analyses"

# Définition des schémas explicites StructType pour l'ingestion propre
MOVIES_SCHEMA = StructType([
    StructField("movieId", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("genres", StringType(), True)
])

RATINGS_SCHEMA = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])


def ingestion(spark):
    """Étape 1a : Lire les données brutes de films et de notes en appliquant des schémas stricts."""
    print("--- [ÉTAPE 1A] Ingestion des données brutes ---")
    
    df_movies = (
        spark.read
        .option("header", "true")
        .option("sep", ",")
        .schema(MOVIES_SCHEMA)
        .csv(MOVIES_BRUT)
    )
    
    df_ratings = (
        spark.read
        .option("header", "true")
        .option("sep", ",")
        .schema(RATINGS_SCHEMA)
        .csv(RATINGS_BRUT)
    )
    
    print("\nSchéma des films (movies) :")
    df_movies.printSchema()
    print(f"Nombre de films bruts : {df_movies.count()}")
    
    print("\nSchéma des notes (ratings) :")
    df_ratings.printSchema()
    print(f"Nombre de notes brutes : {df_ratings.count()}")
    
    return df_movies, df_ratings


def nettoyage(df_movies, df_ratings):
    """Étape 1b : Typer, dériver des colonnes et filtrer les valeurs aberrantes (Bronze -> Silver)."""
    print("\n--- [ÉTAPE 1B] Nettoyage et typage (Bronze -> Silver) ---")
    
    # 1. Nettoyage et colonnes dérivées pour les films
    # - Supprimer les doublons de films
    # - Filtrer les lignes sans identifiant de film ou sans titre
    # - Extraire l'année de sortie à l'aide d'une regex sur le titre (ex: "Toy Story (1995)" -> 1995)
    # Extract movie year with a regex and cast safely (handling cases with no year found)
    year_str = F.regexp_extract(F.col("title"), r"\((\d{4})\)", 1)
    df_movies_clean = (
        df_movies
        .dropDuplicates(["movieId"])
        .filter(F.col("movieId").isNotNull() & F.col("title").isNotNull() & (F.col("title") != ""))
        .withColumn(
            "annee_sortie",
            F.when(year_str != "", year_str.cast(IntegerType())).otherwise(None)
        )
    )
    
    # 2. Nettoyage et colonnes dérivées pour les notes
    # - Supprimer les doublons de notes (userId, movieId)
    # - Filtrer les notes invalides (hors de la plage [0.5, 5.0])
    # - Convertir le timestamp unix en vrai format date/timestamp lisible
    df_ratings_clean = (
        df_ratings
        .dropDuplicates(["userId", "movieId"])
        .filter(
            F.col("userId").isNotNull() &
            F.col("movieId").isNotNull() &
            F.col("rating").isNotNull() &
            (F.col("rating") >= 0.5) & (F.col("rating") <= 5.0)
        )
        .withColumn(
            "date_notation",
            F.from_unixtime(F.col("timestamp")).cast("timestamp")
        )
    )
    
    print(f"Nombre de films propres : {df_movies_clean.count()}")
    print(f"Nombre de notes propres : {df_ratings_clean.count()}")
    
    return df_movies_clean, df_ratings_clean


def ecrire_silver(df_movies, df_ratings):
    """Étape 1c : Écrire la couche intermédiaire Silver nettoyée en Parquet."""
    print("\n--- [ÉTAPE 1C] Écriture de la couche Silver (Parquet) ---")
    
    # Écriture des films
    df_movies.write.mode("overwrite").parquet(SORTIE_SILVER_MOVIES)
    print(f"Couche Silver des films écrite dans {SORTIE_SILVER_MOVIES}")
    
    # Écriture des notes, partitionnée par la note pour démonstration
    # rating a une faible cardinalité (10 valeurs uniques : 0.5, 1.0, ..., 5.0)
    (
        df_ratings.write
        .mode("overwrite")
        .partitionBy("rating")
        .parquet(SORTIE_SILVER_RATINGS)
    )
    print(f"Couche Silver des notes écrite dans {SORTIE_SILVER_RATINGS}")


def transformation_et_analyses(spark):
    """Étape 2 : Relire le propre, effectuer les optimisations de cache/broadcast et exécuter 3 analyses distinctes."""
    print("\n--- [ÉTAPE 2] Relecture Silver et Analyses Métier ---")
    
    # 1. Relecture des données propres
    df_movies = spark.read.parquet(SORTIE_SILVER_MOVIES)
    df_ratings = spark.read.parquet(SORTIE_SILVER_RATINGS)
    
    # --- Optimisation 1 : Cache ---
    # Nous allons comparer les temps d'exécution avec et sans cache sur les notes (ratings) qui est le gros fichier
    print("\n[Mesure d'optimisation] Comparaison de l'effet du cache sur la table des notes :")
    
    # Requêtes répétées sans cache
    start_no_cache = time.time()
    cnt1 = df_ratings.count()
    grp1 = df_ratings.groupBy("movieId").avg("rating").count()
    grp2 = df_ratings.filter(F.col("rating") >= 4.0).count()
    duration_no_cache = time.time() - start_no_cache
    print(f"-> Temps d'exécution sans cache (3 actions Spark) : {duration_no_cache:.3f} secondes")
    
    # Mise en cache du DataFrame ratings
    df_ratings_cached = df_ratings.cache()
    # Force la matérialisation du cache
    start_materialize = time.time()
    df_ratings_cached.count()
    duration_materialize = time.time() - start_materialize
    print(f"-> Temps de matérialisation initiale du cache : {duration_materialize:.3f} secondes")
    
    # Mêmes requêtes avec cache
    start_cache = time.time()
    cnt1_c = df_ratings_cached.count()
    grp1_c = df_ratings_cached.groupBy("movieId").avg("rating").count()
    grp2_c = df_ratings_cached.filter(F.col("rating") >= 4.0).count()
    duration_cache = time.time() - start_cache
    print(f"-> Temps d'exécution avec cache (mêmes actions Spark) : {duration_cache:.3f} secondes")
    print(f"-> Gain de performance grâce au cache : {((duration_no_cache - duration_cache) / duration_no_cache) * 100:.1f}% de réduction du temps.")

    # --- Analyse 1 : Agrégation (groupBy + agg) ---------------------------------
    # Trouver les films les mieux notés avec au moins 50 votes (seuil de représentativité)
    print("\n--- Exécution de l'Analyse 1 : Top 20 des films les mieux notés (>= 50 votes) ---")
    
    movie_stats = (
        df_ratings_cached
        .groupBy("movieId")
        .agg(
            F.count("rating").alias("nombre_votes"),
            F.round(F.avg("rating"), 2).alias("note_moyenne")
        )
        .filter(F.col("nombre_votes") >= 50)
    )
    
    # Jointure avec la table des films pour avoir le titre
    analyse_1 = (
        movie_stats
        .join(F.broadcast(df_movies), on="movieId", how="inner")
        .select("movieId", "title", "nombre_votes", "note_moyenne")
        .orderBy(F.desc("note_moyenne"), F.desc("nombre_votes"))
        .limit(20)
    )
    analyse_1.show(20, truncate=False)

    # --- Analyse 2 : Jointure & Broadcast (join + broadcast + split/explode) -----
    # popularité et score par genre. Un film a ses genres sous forme "Action|Adventure|Sci-Fi"
    # Nous devons découper (split) et exploser (explode) cette chaîne pour agréger par genre individuel.
    print("\n--- Exécution de l'Analyse 2 : Statistiques et popularité par Genre individuel ---")
    
    # Préparation de la table des films avec genre explosé
    df_movies_exploded = (
        df_movies
        .withColumn("genre", F.explode(F.split(F.col("genres"), "\\|")))
    )
    
    # Jointure avec ratings. On utilise un broadcast join sur df_movies_exploded car elle reste de taille modeste.
    analyse_2 = (
        df_ratings_cached
        .join(F.broadcast(df_movies_exploded), on="movieId", how="inner")
        .groupBy("genre")
        .agg(
            F.count("rating").alias("total_notes"),
            F.round(F.avg("rating"), 2).alias("note_moyenne_genre")
        )
        .orderBy(F.desc("total_notes"), F.desc("note_moyenne_genre"))
    )
    analyse_2.show(truncate=False)
    
    # Justification de l'optimisation : Affichage du plan physique de la jointure
    print("\nAffichage du plan d'exécution physique pour l'Analyse 2 (Jointure) :")
    analyse_2.explain()

    # --- Analyse 3 : Window function --------------------------------------------
    # Trouver le top 5 des meilleurs films de chaque genre (pour les films ayant au moins 10 notes)
    print("\n--- Exécution de l'Analyse 3 : Top 5 des films par Genre individuel (Window Function) ---")
    
    # 1. Obtenir les stats moyennes par film
    movie_ratings_summary = (
        df_ratings_cached
        .groupBy("movieId")
        .agg(
            F.count("rating").alias("nombre_votes"),
            F.avg("rating").alias("note_moyenne")
        )
        .filter(F.col("nombre_votes") >= 10)
    )
    
    # 2. Joindre avec les films aux genres explosés
    df_joined_genres = (
        movie_ratings_summary
        .join(F.broadcast(df_movies_exploded), on="movieId", how="inner")
    )
    
    # 3. Définir la fenêtre de classement par genre, trié par note moyenne descendante
    fenetre_genre = Window.partitionBy("genre").orderBy(F.desc("note_moyenne"), F.desc("nombre_votes"))
    
    # 4. Appliquer la fonction de classement et filtrer pour obtenir le top 5
    analyse_3 = (
        df_joined_genres
        .withColumn("rang", F.row_number().over(fenetre_genre))
        .filter(F.col("rang") <= 5)
        .select(
            "genre",
            "rang",
            "title",
            F.round("note_moyenne", 2).alias("note_moyenne"),
            "nombre_votes",
            "annee_sortie"
        )
        .orderBy("genre", "rang")
    )
    
    analyse_3.show(30, truncate=False)
    
    return {"analyse_1": analyse_1, "analyse_2": analyse_2, "analyse_3": analyse_3}


def ecrire_gold(resultats):
    """Étape 3 : Écrire les résultats des analyses dans la couche Gold."""
    print("\n--- [ÉTAPE 3] Écriture de la couche Gold (Résultats d'Analyses) ---")
    for nom, df in resultats.items():
        chemin = f"{SORTIE_GOLD}/{nom}"
        # coalesce(1) est utilisé pour générer un seul fichier de synthèse propre sur le disque, 
        # car les DataFrames d'analyse finale sont de taille réduite.
        df.coalesce(1).write.mode("overwrite").parquet(chemin)
        print(f"Résultat '{nom}' écrit avec succès dans {chemin}")


def main():
    start_time = time.time()
    spark = get_spark("Projet Jour 4 - MovieLens ETL", shuffle_partitions=64)
    print("Spark UI disponible sur http://localhost:4040\n")

    # Étape 1 : Ingestion et nettoyage (Bronze -> Silver)
    movies_brut, ratings_brut = ingestion(spark)
    movies_propre, ratings_propre = nettoyage(movies_brut, ratings_brut)
    ecrire_silver(movies_propre, ratings_propre)

    # Étape 2 : Transformation et analyses (Silver -> Gold)
    resultats = transformation_et_analyses(spark)

    # Étape 3 : Finalisation
    ecrire_gold(resultats)

    total_duration = time.time() - start_time
    print(f"\n==========================================")
    print(f"Pipeline MovieLens exécuté avec succès en {total_duration:.2f} secondes !")
    print(f"==========================================")

    # Garder la session vivante pour explorer la Spark UI
    input("\n[Spark UI] Ouvrez http://localhost:4040 dans votre navigateur pour explorer l'interface.\nAppuyez sur ENTRÉE dans ce terminal pour fermer la session et quitter...")

    # Vider le cache de Spark et fermer la session
    spark.catalog.clearCache()
    spark.stop()


if __name__ == "__main__":
    main()
