"""Profilage qualité de la couche bronze (Spark). Compte exactement les lignes affectées
par chaque problème et écrit le rapport JSON dans reports/quality_report.json."""
import json
import sys
from functools import reduce

from pyspark.sql import functions as F

from common import BASE_SPARK, BRONZE_FILES, IMPACTS, banner, clean_name, spark_session

banner("01_quality_report")
spark = spark_session("agb-01-quality")

rep = {}
dfs = {}
for name, path in BRONZE_FILES.items():
    df = spark.read.csv(f"{BASE_SPARK}/{path}", header=True, multiLine=True, escape='"', inferSchema=False)
    dfs[name] = df
    raw_cols = df.columns
    all_null = reduce(lambda a, b: a & b, [F.col(f"`{c}`").isNull() for c in raw_cols])
    total = df.count()
    empty = df.filter(all_null).count()
    rep[name] = {
        "lignes_brutes": total,
        "lignes_vides": empty,
        "lignes_utiles": total - empty,
        "colonnes": len(raw_cols),
        "noms_colonnes_anormaux": [repr(c) for c in raw_cols if clean_name(c) != c],
    }

# --- synthèse ---
s = dfs["synthese"]
s = s.toDF(*[clean_name(c) for c in s.columns]).dropna(how="all")
r = rep["synthese"]
r["code_agb_non_numerique"] = s.filter(~F.col("Code AGB").rlike("^[0-9]+$")).count()
r["exemples_code_agb_non_numerique"] = [x[0] for x in s.filter(~F.col("Code AGB").rlike("^[0-9]+$")).select("Code AGB").limit(3).collect()]
r["code_agb_dupliques"] = s.count() - s.select("Code AGB").distinct().count()
dup_ciq = s.groupBy("Code CIQUAL").count().filter("count > 1")
r["codes_ciqual_partages"] = dup_ciq.count()
r["lignes_avec_code_ciqual_partage"] = int(dup_ciq.agg(F.sum("count")).first()[0] or 0)
for col in ["Préparation", "Livraison", "Approche emballage"]:
    c = F.col(f"`{col}`")
    r[f"espaces_parasites_{col}"] = s.filter(c != F.trim(c)).count()
    r[f"modalites_{col}_avant_apres_trim"] = [s.select(col).distinct().count(), s.select(F.trim(c)).distinct().count()]
neg_cols = list(IMPACTS) + [
    "Changement climatique - émissions biogéniques",
    "Changement climatique - émissions fossiles",
    "Changement climatique - émissions liées au changement d'affectation des sols",
]
neg_by_col = {}
for col in neg_cols:
    n = s.filter(F.col(f"`{col}`").cast("double") < 0).count()
    if n:
        neg_by_col[col] = n
r["valeurs_negatives_par_colonne"] = neg_by_col
any_neg = reduce(lambda a, b: a | b, [F.col(f"`{c}`").cast("double") < 0 for c in neg_cols])
r["lignes_avec_valeur_negative"] = s.filter(any_neg).count()
cc = F.col("`Changement climatique`").cast("double")
decomp = sum(F.col(f"`{c}`").cast("double") for c in neg_cols[-3:])
r["decomposition_cc_ecart_sup_1pct"] = s.filter(F.abs(decomp - cc) / cc > 0.01).count()

# --- détail par ingrédient ---
d = dfs["detail"]
d = d.toDF(*[clean_name(c) for c in d.columns]).dropna(how="all")
r = rep["detail"]
ing = F.col("Ingredients")
r["ingredients_espaces_parasites"] = d.filter(ing != F.trim(ing)).count()
r["exemples_ingredients_espaces"] = [repr(x[0]) for x in d.filter(ing != F.trim(ing)).select("Ingredients").distinct().limit(3).collect()]
r["doublons_produit_ingredient"] = d.count() - d.select("Ciqual AGB", F.lower(F.trim(ing))).distinct().count()
r["produits_detailles"] = d.select("Ciqual AGB").distinct().count()
r["produits_absents_de_la_synthese"] = d.select("Ciqual AGB").distinct().join(
    s.select(F.col("Code AGB").alias("Ciqual AGB")), "Ciqual AGB", "left_anti").count()
r["lignes_avec_valeur_negative"] = d.filter(
    reduce(lambda a, b: a | b, [F.col(f"`{c}`").cast("double") < 0 for c in IMPACTS])).count()
rep["synthese"]["produits_sans_detail_ingredient"] = s.select("Code AGB").distinct().join(
    d.select(F.col("Ciqual AGB").alias("Code AGB")).distinct(), "Code AGB", "left_anti").count()

out = json.dumps(rep, ensure_ascii=False, indent=2)
print(out)
spark.createDataFrame([(out,)], ["json"]).coalesce(1).write.mode("overwrite").text(f"{BASE_SPARK}/reports/quality_report")
spark.stop()
sys.exit(0)
