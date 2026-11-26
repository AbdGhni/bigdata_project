"""Vérification indépendante (DuckDB, exécutée dans un pod Kubernetes séparé).
Repart des CSV BRONZE (pas de la couche silver), avec un autre moteur et un code de
parsing distinct, et recalcule trois résultats clés produits par Spark en gold :
  1. médiane d'empreinte carbone par groupe d'aliments
  2. ingrédient n°1 de chaque groupe et sa part
  3. nombre de produits dont la somme des ingrédients s'écarte de > 1 % du total officiel
Code retour 1 si un écart est détecté."""
import json
import os
import sys

import duckdb

from common import BASE_DUCK, BRONZE_FILES, ON_S3, TOLERANCE_REL, banner, clean_name

banner("04_verify_duckdb")
con = duckdb.connect()
if ON_S3:
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""CREATE OR REPLACE SECRET s3v (TYPE s3,
        KEY_ID '{os.environ["AWS_ACCESS_KEY_ID"]}', SECRET '{os.environ["AWS_SECRET_ACCESS_KEY"]}',
        SESSION_TOKEN '{os.environ.get("AWS_SESSION_TOKEN", "")}',
        ENDPOINT '{os.environ["AWS_S3_ENDPOINT"]}', URL_STYLE 'path',
        REGION '{os.environ.get("AWS_DEFAULT_REGION", "us-east-1")}')""")


def load_bronze(key, view, wanted):
    path = f"{BASE_DUCK}/{BRONZE_FILES[key]}"
    con.execute(f"CREATE OR REPLACE VIEW raw_{view} AS SELECT * FROM read_csv('{path}', header=true, all_varchar=true)")
    cols = [r[0] for r in con.execute(f"DESCRIBE raw_{view}").fetchall()]
    by_clean = {clean_name(c): c for c in cols}
    sel = ", ".join(f'"{by_clean[k]}" AS {v}' for k, v in wanted.items())
    first = by_clean[next(iter(wanted))]
    con.execute(f'CREATE OR REPLACE TABLE {view} AS SELECT {sel} FROM raw_{view} WHERE "{first}" IS NOT NULL')


load_bronze("synthese", "s", {"Code AGB": "code", "Groupe d'aliment": "groupe",
                              "Changement climatique": "cc"})
load_bronze("detail", "d", {"Ciqual AGB": "code", "Groupe d'aliment": "groupe",
                            "Ingredients": "ingredient", "Changement climatique": "cc"})

checks = {}

# 1. médiane par groupe
ref = dict(con.execute("SELECT trim(groupe), round(median(cc::DOUBLE), 3) FROM s GROUP BY 1").fetchall())
gold = dict(con.execute(f"SELECT groupe, cc_mediane_kgco2e_kg FROM read_parquet('{BASE_DUCK}/gold/groupes/*.parquet')").fetchall())
diff = {g: (ref.get(g), gold.get(g)) for g in set(ref) | set(gold) if ref.get(g) is None or gold.get(g) is None or abs(ref[g] - gold[g]) > 1e-9}
checks["mediane_par_groupe"] = {"groupes": len(ref), "ecarts": diff}

# 2. ingrédient n°1 par groupe
ref = {r[0]: (r[1], round(r[2], 4)) for r in con.execute("""
    WITH c AS (SELECT trim(groupe) g, lower(trim(ingredient)) i, sum(cc::DOUBLE) v FROM d GROUP BY 1, 2)
    SELECT g, arg_max(i, v) FILTER (WHERE i <> 'autres étapes'),
           max(v) FILTER (WHERE i <> 'autres étapes') / sum(v) FROM c GROUP BY g""").fetchall()}
gold = {r[0]: (r[1], r[2]) for r in con.execute(f"""
    SELECT groupe, ingredient, part_du_groupe FROM read_parquet('{BASE_DUCK}/gold/ingredients_points_chauds/*.parquet')
    WHERE rang = 1""").fetchall()}
diff = {g: (ref.get(g), gold.get(g)) for g in set(ref) | set(gold)
        if ref.get(g) is None or gold.get(g) is None or ref[g][0] != gold[g][0] or abs(ref[g][1] - gold[g][1]) > 1e-4}
checks["ingredient_principal_par_groupe"] = {"groupes": len(ref), "ecarts": diff,
                                             "resultat": {g: v for g, v in sorted(ref.items())}}

# 3. réconciliation détail / synthèse
n_ref = con.execute(f"""
    WITH a AS (SELECT code, sum(cc::DOUBLE) v FROM d GROUP BY code)
    SELECT count(*) FILTER (WHERE abs(a.v - s.cc::DOUBLE) / s.cc::DOUBLE > {TOLERANCE_REL}), count(*)
    FROM a JOIN s USING (code)""").fetchone()
n_gold = con.execute(f"""SELECT count(*) FILTER (WHERE hors_tolerance), count(*)
    FROM read_parquet('{BASE_DUCK}/gold/reconciliation/*.parquet')""").fetchone()
checks["reconciliation"] = {"duckdb_bronze": list(n_ref), "spark_gold": list(n_gold), "ecarts": {} if n_ref == n_gold else {"n": [n_ref, n_gold]}}

ok = all(not c["ecarts"] for c in checks.values())
checks["statut"] = "OK" if ok else "ECHEC"
out = json.dumps(checks, ensure_ascii=False, indent=2, default=str)
print(out)
if not ON_S3:
    os.makedirs(f"{BASE_DUCK}/reports", exist_ok=True)
con.execute(f"COPY (SELECT ? AS rapport) TO '{BASE_DUCK}/reports/verification_duckdb.json' (FORMAT json)", [out])
print(f"VERIFICATION {checks['statut']}")
sys.exit(0 if ok else 1)
