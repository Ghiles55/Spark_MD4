from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TP01 - RDD et paresse")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

sc = spark.sparkContext

nombres = sc.parallelize(range(1, 21), numSlices=4)

# À compléter : afficher le nombre de partitions
print("Partitions :", nombres.getNumPartitions())

# À compléter : ramener tous les éléments vers le driver (action)
print("Contenu :", nombres.collect())

carres = nombres.map(lambda x: x * x)
carres_pairs = carres.filter(lambda x: x % 2 == 0)

# À compléter : récupérer le résultat (action)
print("Carres pairs :", carres_pairs.collect())

def trace(x):
    print("  -> je calcule", x)     # effet de bord visible
    return x * 10

paresseux = nombres.map(trace)
print("La transformation est definie, mais rien ne s'est encore affiche.")

# À compléter : maintenant, déclenchez une action et observez les traces apparaître
resultat = paresseux.collect()
print("Resultat :", resultat)

phrases = sc.parallelize([
    "le taxi jaune roule",
    "le taxi attend le client",
])

# map : une liste de mots par phrase (resultat imbrique)
par_map = phrases.map(lambda p: p.split(" "))

# flatMap : tous les mots a plat
par_flatmap = phrases.flatMap(lambda p: p.split(" "))

print("map      :", par_map.collect())
print("flatMap  :", par_flatmap.collect())