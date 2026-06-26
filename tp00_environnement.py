from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TP00 - Environnement")
    .master("local[*]")            # mode local, tous les coeurs
    .getOrCreate()
)

# Reduire le bruit dans la console
spark.sparkContext.setLogLevel("WARN")

print("Version de Spark :", spark.version)
print("Master :", spark.sparkContext.master)

chemin = "data/datasets/yellow_tripdata_2024-01.parquet"

df = spark.read.parquet(chemin)

# A completer : afficher 5 lignes sans tronquer les colonnes
df.show(5,truncate=False)

# A completer : afficher le schema (types de chaque colonne)
df.printSchema()

# A completer : compter le nombre de lignes
nb = df.count()
print("Nombre de courses :", nb)

input("Spark UI sur http://localhost:4040 - appuyez sur Entree pour quitter...")

spark.stop()