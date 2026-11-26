"""Silver -> Gold (Spark SQL). Tables finales répondant à la problématique :
  gold/ingredients_points_chauds : par groupe, part de l'empreinte carbone portée par chaque ingrédient
  gold/groupes                   : statistiques d'empreinte carbone par groupe d'aliments
  gold/top_produits              : 5 produits les plus émetteurs par groupe
  gold/reconciliation            : somme des ingrédients vs total officiel, par produit
"""
from common import BASE_SPARK, TOLERANCE_REL, banner, spark_session

banner("03_gold")
spark = spark_session("agb-03-gold")
spark.read.parquet(f"{BASE_SPARK}/silver/synthese").createOrReplaceTempView("synthese")
spark.read.parquet(f"{BASE_SPARK}/silver/detail_ingredient").createOrReplaceTempView("detail")

queries = {
    "reconciliation": f"""
        SELECT s.code_agb, s.nom_produit, s.groupe,
               COUNT(*)                         AS nb_ingredients,
               SUM(d.cc_kgco2e)                 AS cc_somme_ingredients,
               FIRST(s.cc_kgco2e)               AS cc_total_officiel,
               ABS(SUM(d.cc_kgco2e) - FIRST(s.cc_kgco2e)) / FIRST(s.cc_kgco2e) AS ecart_relatif,
               ABS(SUM(d.cc_kgco2e) - FIRST(s.cc_kgco2e)) / FIRST(s.cc_kgco2e) > {TOLERANCE_REL} AS hors_tolerance
        FROM detail d JOIN synthese s ON d.code_agb = s.code_agb
        GROUP BY s.code_agb, s.nom_produit, s.groupe""",
    "ingredients_points_chauds": """
        WITH contrib AS (
            SELECT d.groupe, d.ingredient_norm AS ingredient,
                   COUNT(DISTINCT d.code_agb) AS nb_produits,
                   SUM(d.cc_kgco2e)           AS cc_cumule
            FROM detail d GROUP BY d.groupe, d.ingredient_norm)
        SELECT groupe, ingredient, nb_produits,
               ingredient = 'autres étapes' AS est_etapes_aval,
               ROUND(cc_cumule, 4) AS cc_cumule_kgco2e,
               ROUND(cc_cumule / SUM(cc_cumule) OVER (PARTITION BY groupe), 4) AS part_du_groupe,
               CASE WHEN ingredient <> 'autres étapes' THEN
                    RANK() OVER (PARTITION BY groupe, ingredient = 'autres étapes' ORDER BY cc_cumule DESC)
               END AS rang
        FROM contrib""",
    "groupes": """
        WITH top_ing AS (
            SELECT code_agb,
                   MAX(CASE WHEN ingredient_norm <> 'autres étapes' THEN cc_kgco2e END) / SUM(cc_kgco2e) AS part_ingredient_principal,
                   SUM(CASE WHEN ingredient_norm = 'autres étapes' THEN cc_kgco2e ELSE 0 END) / SUM(cc_kgco2e) AS part_etapes_aval
            FROM detail GROUP BY code_agb)
        SELECT s.groupe,
               COUNT(*)                                        AS nb_produits,
               COUNT(t.code_agb)                               AS nb_produits_detailles,
               ROUND(PERCENTILE(s.cc_kgco2e, 0.5), 3)          AS cc_mediane_kgco2e_kg,
               ROUND(AVG(s.cc_kgco2e), 3)                      AS cc_moyenne_kgco2e_kg,
               ROUND(PERCENTILE(s.cc_kgco2e, 0.9), 3)          AS cc_p90_kgco2e_kg,
               ROUND(SUM(s.cc_fossile) / SUM(s.cc_kgco2e), 3)  AS part_fossile,
               ROUND(SUM(s.cc_biogenique) / SUM(s.cc_kgco2e), 3) AS part_biogenique,
               ROUND(SUM(s.cc_affectation_sols) / SUM(s.cc_kgco2e), 3) AS part_affectation_sols,
               ROUND(PERCENTILE(t.part_ingredient_principal, 0.5), 3) AS mediane_part_ingredient_principal,
               ROUND(PERCENTILE(t.part_etapes_aval, 0.5), 3)  AS mediane_part_etapes_aval,
               ROUND(AVG(s.dqr), 2)                            AS dqr_moyen
        FROM synthese s LEFT JOIN top_ing t ON s.code_agb = t.code_agb
        GROUP BY s.groupe""",
    "top_produits": """
        SELECT * FROM (
            SELECT groupe, code_agb, nom_produit, cc_kgco2e, dqr,
                   ROW_NUMBER() OVER (PARTITION BY groupe ORDER BY cc_kgco2e DESC, code_agb) AS rang
            FROM synthese) WHERE rang <= 5""",
}

for name, sql in queries.items():
    df = spark.sql(sql)
    df.write.mode("overwrite").parquet(f"{BASE_SPARK}/gold/{name}")
    print(f"gold/{name}: {spark.read.parquet(f'{BASE_SPARK}/gold/{name}').count()} lignes")

print("\n--- Groupes (tri par médiane) ---")
spark.read.parquet(f"{BASE_SPARK}/gold/groupes").orderBy("cc_mediane_kgco2e_kg", ascending=False).show(20, truncate=False)
print("--- Top 3 ingrédients par groupe ---")
(spark.read.parquet(f"{BASE_SPARK}/gold/ingredients_points_chauds").filter("rang <= 3")
      .orderBy("groupe", "rang").show(40, truncate=False))
print("--- Réconciliation ---")
spark.read.parquet(f"{BASE_SPARK}/gold/reconciliation").groupBy("hors_tolerance").count().show()
spark.stop()
