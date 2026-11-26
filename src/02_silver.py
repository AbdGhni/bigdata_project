"""Bronze -> Silver (Spark) : suppression des lignes vides, noms de colonnes normalisés,
typage, nettoyage des libellés, déduplication, indicateurs qualité. Écriture Parquet
en mode overwrite (relançable sans effet de bord)."""
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import (BASE_SPARK, BRONZE_FILES, IMPACTS, RENAME_DETAIL, RENAME_SYNTHESE,
                    banner, clean_name, spark_session)

banner("02_silver")
spark = spark_session("agb-02-silver")


def read_bronze(key, rename):
    df = spark.read.csv(f"{BASE_SPARK}/{BRONZE_FILES[key]}", header=True, multiLine=True,
                        escape='"', inferSchema=False)
    df = df.toDF(*[clean_name(c) for c in df.columns]).dropna(how="all")
    missing = set(rename) - set(df.columns)
    if missing:
        raise SystemExit(f"Colonnes attendues absentes dans {key}: {missing}")
    return df.select([F.col(f"`{k}`").alias(v) for k, v in rename.items()])


IMPACT_COLS = list(IMPACTS.values())
TEXT_COLS = ["groupe", "sous_groupe", "nom_produit", "lci_name"]

# --- synthèse : 1 ligne par produit (code_agb) ---
s = read_bronze("synthese", RENAME_SYNTHESE)
for c in TEXT_COLS + ["livraison", "emballage", "preparation", "code_agb"]:
    s = s.withColumn(c, F.trim(F.col(c)))
for c in IMPACT_COLS + ["dqr", "cc_biogenique", "cc_fossile", "cc_affectation_sols"]:
    s = s.withColumn(c, F.col(c).cast("double"))
s = (s.withColumn("code_ciqual", F.col("code_ciqual").cast("double").cast("int"))
       .withColumn("code_saison", F.col("code_saison").cast("int"))
       .withColumn("code_avion", F.col("code_avion").cast("int"))
       .dropDuplicates(["code_agb"]))
neg = F.lit(False)
for c in IMPACT_COLS + ["cc_affectation_sols"]:
    neg = neg | (F.col(c) < 0)
w = F.count("*").over(Window.partitionBy("code_ciqual"))
s = (s.withColumn("flag_valeur_negative", neg)
       .withColumn("flag_ciqual_partage", w > 1)
       .withColumn("cc_kgco2e_par_100g", F.col("cc_kgco2e") / 10))

# --- détail : 1 ligne par (produit, ingrédient) ---
d = read_bronze("detail", RENAME_DETAIL)
for c in TEXT_COLS + ["ingredient", "code_agb"]:
    d = d.withColumn(c, F.trim(F.col(c)))
for c in IMPACT_COLS:
    d = d.withColumn(c, F.col(c).cast("double"))
d = (d.withColumn("code_ciqual", F.col("code_ciqual").cast("double").cast("int"))
       .withColumn("ingredient_norm", F.lower(F.col("ingredient")))
       .dropDuplicates(["code_agb", "ingredient_norm"]))

for name, df in [("synthese", s), ("detail_ingredient", d)]:
    df.write.mode("overwrite").parquet(f"{BASE_SPARK}/silver/{name}")
    back = spark.read.parquet(f"{BASE_SPARK}/silver/{name}")
    print(f"silver/{name}: {back.count()} lignes, {len(back.columns)} colonnes")
spark.stop()
