from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("TP02 - Premier DataFrame")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

chemin = "data/datasets/yellow_tripdata_2024-01.parquet"

# A completer : lire le fichier Parquet dans un DataFrame nomme df
df = spark.read.parquet(chemin)

print("Type de df :", type(df))

# A completer : afficher le schema (nom et type de chaque colonne)
df.printSchema()

# A completer : afficher 5 lignes sans tronquer les colonnes
df.show(5, truncate=False)

# A completer : lister juste les noms de colonnes (attribut Python, pas une methode)
print("Colonnes :", df.columns)

# A completer : ne garder que ces 5 colonnes
colonnes = ["tpep_pickup_datetime", "trip_distance", "PULocationID", "tip_amount", "total_amount"]
df_court = df.select(colonnes)

df_court.show(5, truncate=False)

df_valides = df.filter(
    (F.col("passenger_count") > 0) & (F.col("trip_distance") > 0)
)

print("Courses valides :", df_valides.count())

# A completer : statistiques sur ces trois colonnes
df.describe("trip_distance", "fare_amount", "total_amount").show()

# Indice : groupBy() sans argument + agg, ou directement select + agg
df_valides.agg(F.avg("trip_distance").alias("distance_moyenne")).show()

df_valides.agg(
    F.max("trip_distance").alias("distance_max"),
    F.max("total_amount").alias("montant_max")
).show()