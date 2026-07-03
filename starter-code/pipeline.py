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

# On définit le schéma à la main (StructType) au lieu de laisser Spark le deviner.
# C'est plus fiable et plus rapide.
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
    """Étape 1a : on lit les deux fichiers CSV bruts (films et notes)."""
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
    """Étape 1b : on nettoie les deux tables et on ajoute des colonnes utiles (Bronze -> Silver)."""
    print("\n--- [ÉTAPE 1B] Nettoyage et typage (Bronze -> Silver) ---")
    
    # Nettoyage des films :
    # - on enlève les doublons de movieId
    # - on enlève les lignes sans id ou sans titre
    # - on récupère l'année dans le titre avec une regex, ex: "Toy Story (1995)" -> 1995
    #   si le titre n'a pas d'année, on met juste None (pas d'erreur)
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
    
    # Nettoyage des notes :
    # - on enlève les doublons (même utilisateur + même film)
    # - on garde seulement les notes valides, entre 0.5 et 5.0
    # - on transforme le timestamp (nombre) en vraie date lisible
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
    """Étape 1c : on écrit les données nettoyées en Parquet (couche Silver)."""
    print("\n--- [ÉTAPE 1C] Écriture de la couche Silver (Parquet) ---")
    
    # Écriture des films
    df_movies.write.mode("overwrite").parquet(SORTIE_SILVER_MOVIES)
    print(f"Couche Silver des films écrite dans {SORTIE_SILVER_MOVIES}")
    
    # Pour les notes, on partitionne par rating (10 valeurs possibles seulement,
    # donc pas trop de dossiers créés)
    (
        df_ratings.write
        .mode("overwrite")
        .partitionBy("rating")
        .parquet(SORTIE_SILVER_RATINGS)
    )
    print(f"Couche Silver des notes écrite dans {SORTIE_SILVER_RATINGS}")


def transformation_et_analyses(spark):
    """Étape 2 : on relit les données propres, on teste le cache, et on fait 3 analyses."""
    print("\n--- [ÉTAPE 2] Relecture Silver et Analyses Métier ---")
    
    # On relit les fichiers Parquet nettoyés
    df_movies = spark.read.parquet(SORTIE_SILVER_MOVIES)
    df_ratings = spark.read.parquet(SORTIE_SILVER_RATINGS)
    
    # --- Test du cache ---
    # On veut comparer le temps avec et sans cache, sur la table ratings
    # qui est utilisée plusieurs fois par la suite.
    print("\n[Mesure d'optimisation] Comparaison de l'effet du cache sur la table des notes :")
    
    # D'abord sans cache : on fait 3 actions et on chronomètre
    start_no_cache = time.time()
    cnt1 = df_ratings.count()
    grp1 = df_ratings.groupBy("movieId").avg("rating").count()
    grp2 = df_ratings.filter(F.col("rating") >= 4.0).count()
    duration_no_cache = time.time() - start_no_cache
    print(f"-> Temps d'exécution sans cache (3 actions Spark) : {duration_no_cache:.3f} secondes")
    
    # On met la table en cache
    df_ratings_cached = df_ratings.cache()
    # count() ici sert juste à forcer Spark à vraiment charger le cache tout de suite
    start_materialize = time.time()
    df_ratings_cached.count()
    duration_materialize = time.time() - start_materialize
    print(f"-> Temps de matérialisation initiale du cache : {duration_materialize:.3f} secondes")
    
    # On refait les mêmes 3 actions, mais cette fois sur la version en cache
    start_cache = time.time()
    cnt1_c = df_ratings_cached.count()
    grp1_c = df_ratings_cached.groupBy("movieId").avg("rating").count()
    grp2_c = df_ratings_cached.filter(F.col("rating") >= 4.0).count()
    duration_cache = time.time() - start_cache
    print(f"-> Temps d'exécution avec cache (mêmes actions Spark) : {duration_cache:.3f} secondes")
    print(f"-> Gain de performance grâce au cache : {((duration_no_cache - duration_cache) / duration_no_cache) * 100:.1f}% de réduction du temps.")

    # --- Analyse 1 : agrégation -------------------------------------------------
    # But : trouver les films les mieux notés, en gardant seulement ceux
    # qui ont au moins 50 votes (sinon un film avec une seule note de 5 fausserait le classement)
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
    
    # On rejoint avec la table des films pour avoir le titre
    analyse_1 = (
        movie_stats
        .join(F.broadcast(df_movies), on="movieId", how="inner")
        .select("movieId", "title", "nombre_votes", "note_moyenne")
        .orderBy(F.desc("note_moyenne"), F.desc("nombre_votes"))
        .limit(20)
    )
    analyse_1.show(20, truncate=False)

    # --- Analyse 2 : jointure avec broadcast -------------------------------------
    # But : popularité et note moyenne par genre.
    # Problème : un film a plusieurs genres collés dans une seule colonne
    # ("Action|Adventure|Sci-Fi"), donc on doit d'abord les séparer (split)
    # puis dupliquer une ligne par genre (explode).
    print("\n--- Exécution de l'Analyse 2 : Statistiques et popularité par Genre individuel ---")
    
    # On prépare une table films où chaque ligne = un seul genre
    df_movies_exploded = (
        df_movies
        .withColumn("genre", F.explode(F.split(F.col("genres"), "\\|")))
    )
    
    # Jointure avec les notes. On utilise broadcast car cette table de films
    # reste petite, même après l'explosion des genres. Ça évite un gros shuffle.
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
    
    # On affiche le plan d'exécution pour vérifier que c'est bien un broadcast join
    print("\nAffichage du plan d'exécution physique pour l'Analyse 2 (Jointure) :")
    analyse_2.explain()

    # --- Analyse 3 : window function ---------------------------------------------
    # But : top 5 des films les mieux notés, par genre (au moins 10 notes chacun)
    print("\n--- Exécution de l'Analyse 3 : Top 5 des films par Genre individuel (Window Function) ---")
    
    # D'abord on calcule la note moyenne et le nombre de votes par film
    movie_ratings_summary = (
        df_ratings_cached
        .groupBy("movieId")
        .agg(
            F.count("rating").alias("nombre_votes"),
            F.avg("rating").alias("note_moyenne")
        )
        .filter(F.col("nombre_votes") >= 10)
    )
    
    # On rejoint avec les films (genres explosés) pour avoir le titre et le genre
    df_joined_genres = (
        movie_ratings_summary
        .join(F.broadcast(df_movies_exploded), on="movieId", how="inner")
    )
    
    # On crée une fenêtre : un classement par genre, du mieux noté au moins bien noté
    fenetre_genre = Window.partitionBy("genre").orderBy(F.desc("note_moyenne"), F.desc("nombre_votes"))
    
    # On numérote chaque film dans son genre, et on garde seulement le top 5
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
    """Étape 3 : on écrit les résultats des 3 analyses (couche Gold)."""
    print("\n--- [ÉTAPE 3] Écriture de la couche Gold (Résultats d'Analyses) ---")
    for nom, df in resultats.items():
        chemin = f"{SORTIE_GOLD}/{nom}"
        # coalesce(1) = un seul fichier de sortie. On peut se le permettre ici
        # car ces résultats sont petits (top 20, top 5 par genre...).
        df.coalesce(1).write.mode("overwrite").parquet(chemin)
        print(f"Résultat '{nom}' écrit avec succès dans {chemin}")


def exploration_pushdown(spark):
    """Étape 4 : notre exploration - est-ce que Spark lit vraiment moins de fichiers
    quand on filtre sur la colonne de partition ?

    La table ratings est partitionnée par rating (10 valeurs : 0.5, 1.0, ..., 5.0),
    donc sur le disque il y a un dossier par note.

    Idée : si on filtre rating >= 4.0, Spark ne devrait avoir besoin d'ouvrir que
    les dossiers concernés (4.0, 4.5, 5.0), pas les 10. C'est le "partition pruning".

    On compare une lecture sans filtre et une lecture avec filtre, et on regarde
    le plan d'exécution pour voir si Spark a bien fait le tri.
    """
    print("\n--- [ÉTAPE 4] Exploration : partition pruning sur la table ratings ---")

    # Attention : la table ratings a été mise en cache à l'étape 2.
    # Si on ne vide pas ce cache, Spark va réutiliser la version en mémoire
    # au lieu de relire le Parquet, et notre test ne voudrait plus rien dire.
    spark.catalog.clearCache()

    # Lecture complète, sans filtre
    start_complet = time.time()
    df_complet = spark.read.parquet(SORTIE_SILVER_RATINGS)
    nb_lignes_complet = df_complet.count()
    duree_complet = time.time() - start_complet
    nb_fichiers_complet = len(df_complet.inputFiles())

    # Lecture avec filtre sur la colonne de partition (rating)
    start_filtre = time.time()
    df_filtre = spark.read.parquet(SORTIE_SILVER_RATINGS).filter(F.col("rating") >= 4.0)
    nb_lignes_filtre = df_filtre.count()
    duree_filtre = time.time() - start_filtre
    nb_fichiers_filtre = len(df_filtre.inputFiles())

    print(f"Lecture complète : {nb_lignes_complet} lignes, {nb_fichiers_complet} fichiers lus, {duree_complet:.3f}s")
    print(f"Lecture filtrée (rating >= 4.0) : {nb_lignes_filtre} lignes, {nb_fichiers_filtre} fichiers lus, {duree_filtre:.3f}s")

    # On regarde le plan d'exécution : la ligne "PartitionFilters" prouve
    # que Spark a bien éliminé des dossiers avant de lire les fichiers
    print("\nPlan d'exécution de la lecture filtrée (on cherche la ligne PartitionFilters) :")
    df_filtre.explain()


def main():
    start_time = time.time()
    spark = get_spark("Projet Jour 4 - MovieLens ETL", shuffle_partitions=64)
    print("Spark UI disponible sur http://localhost:4040\n")

    # Étape 1 : on lit et on nettoie les données brutes
    movies_brut, ratings_brut = ingestion(spark)
    movies_propre, ratings_propre = nettoyage(movies_brut, ratings_brut)
    ecrire_silver(movies_propre, ratings_propre)

    # Étape 2 : les 3 analyses + les optimisations
    resultats = transformation_et_analyses(spark)

    # Étape 3 : on écrit les résultats
    ecrire_gold(resultats)

    # Étape 4 : notre exploration en plus du cours
    exploration_pushdown(spark)

    total_duration = time.time() - start_time
    print(f"\n==========================================")
    print(f"Pipeline MovieLens exécuté avec succès en {total_duration:.2f} secondes !")
    print(f"==========================================")

    # On garde la session ouverte pour pouvoir regarder la Spark UI tranquillement
    input("\n[Spark UI] Ouvrez http://localhost:4040 dans votre navigateur pour explorer l'interface.\nAppuyez sur ENTRÉE dans ce terminal pour fermer la session et quitter...")

    # On ferme proprement la session Spark
    spark.catalog.clearCache()
    spark.stop()


if __name__ == "__main__":
    main()