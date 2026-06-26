"""Tests unitaires automatisés pour le pipeline Spark MovieLens.

Rendu par le groupe de 3 personnes.
Ces tests valident les fonctions de nettoyage, d'extraction d'année et de validation des notes.
"""

import sys
import os
import unittest

# Ajouter le dossier starter-code au chemin de recherche pour importer les modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'starter-code')))

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, LongType
)
from pipeline import nettoyage


class TestMovieLensDataPipeline(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """Initialise une session Spark locale pour la durée des tests."""
        cls.spark = (
            SparkSession.builder
            .appName("UnitTest-MovieLens")
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("WARN")

    @classmethod
    def tearDownClass(cls):
        """Arrête la session Spark à la fin des tests."""
        cls.spark.stop()

    def test_nettoyage_movies_et_ratings(self):
        """Valide le nettoyage des doublons, le filtrage des notes invalides, et l'extraction d'année par regex."""
        
        # Données de test pour les films :
        # 1. Un film valide avec année de sortie : "Toy Story (1995)"
        # 2. Un doublon de ce film (doit être dédoublonné)
        # 3. Un film sans année de sortie dans le titre (l'année doit être extraite comme null)
        # 4. Un film invalide avec titre vide (doit être filtré)
        test_movies = [
            (1, "Toy Story (1995)", "Adventure|Animation|Children|Comedy|Fantasy"),
            (1, "Toy Story (1995)", "Adventure|Animation|Children|Comedy|Fantasy"),  # Doublon
            (2, "Jumanji", "Adventure|Children|Fantasy"),                               # Pas d'année
            (3, "", "Comedy"),                                                         # Titre vide (filtré)
            (4, None, "Drama")                                                         # Titre null (filtré)
        ]
        
        movies_schema = StructType([
            StructField("movieId", IntegerType(), True),
            StructField("title", StringType(), True),
            StructField("genres", StringType(), True)
        ])
        
        # Données de test pour les notes (ratings) :
        # 1. Une note valide (UserId 1, MovieId 1, Note 4.0)
        # 2. Un doublon de note (doit être dédoublonné)
        # 3. Une note supérieure à 5.0 (doit être filtrée)
        # 4. Une note inférieure à 0.5 (doit être filtrée)
        # 5. Une ligne avec MovieId null (doit être filtrée)
        test_ratings = [
            (1, 1, 4.0, 1000000000),  # Valide
            (1, 1, 4.0, 1000000000),  # Doublon
            (1, 2, 6.0, 1000000005),  # Invalide (> 5)
            (1, 3, 0.2, 1000000010),  # Invalide (< 0.5)
            (2, None, 3.5, 1000000015) # MovieId null
        ]
        
        ratings_schema = StructType([
            StructField("userId", IntegerType(), True),
            StructField("movieId", IntegerType(), True),
            StructField("rating", DoubleType(), True),
            StructField("timestamp", LongType(), True)
        ])
        
        df_movies_in = self.spark.createDataFrame(test_movies, schema=movies_schema)
        df_ratings_in = self.spark.createDataFrame(test_ratings, schema=ratings_schema)
        
        # Exécution de la fonction de nettoyage
        df_movies_out, df_ratings_out = nettoyage(df_movies_in, df_ratings_in)
        
        movies_results = df_movies_out.collect()
        ratings_results = df_ratings_out.collect()
        
        # --- Vérifications sur les Films ---
        # Films conservés : MovieId 1 ("Toy Story") et MovieId 2 ("Jumanji")
        self.assertEqual(len(movies_results), 2, f"Attendu : 2 films, Obtenu : {len(movies_results)}")
        
        movies_map = {row["movieId"]: row for row in movies_results}
        
        self.assertIn(1, movies_map)
        self.assertEqual(movies_map[1]["annee_sortie"], 1995)  # Année extraite de "(1995)"
        
        self.assertIn(2, movies_map)
        self.assertIsNone(movies_map[2]["annee_sortie"])       # Année absente -> None/Null
        
        self.assertNotIn(3, movies_map)
        self.assertNotIn(4, movies_map)
        
        # --- Vérifications sur les Notes ---
        # Notes conservées : seulement la première (UserId 1, MovieId 1, Note 4.0)
        self.assertEqual(len(ratings_results), 1, f"Attendu : 1 note, Obtenu : {len(ratings_results)}")
        self.assertEqual(ratings_results[0]["userId"], 1)
        self.assertEqual(ratings_results[0]["movieId"], 1)
        self.assertEqual(ratings_results[0]["rating"], 4.0)
        self.assertIsNotNone(ratings_results[0]["date_notation"])  # Timestamp converti


if __name__ == "__main__":
    unittest.main()
