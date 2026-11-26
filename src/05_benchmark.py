"""Mesures comparatives des alternatives (à lancer dans le service vscode-pyspark).
Même calcul (agrégation ingrédients x groupe + réconciliation détail/synthèse) exécuté par
pandas, DuckDB et Spark, sur CSV et Parquet, aux échelles x1, x10 et x100 (données
répliquées avec des codes produits suffixés). Chaque mesure tourne dans un sous-processus
séparé : temps total (import/démarrage du moteur inclus), temps lecture+calcul, pic mémoire RSS.
Sortie : reports/benchmark.csv (+ copie S3 par run_all.sh)."""
import csv
import os
import subprocess
import sys
import time

import pandas as pd

from common import BASE_SPARK, banner

HERE = os.path.dirname(os.path.abspath(__file__))
WORK = os.environ.get("BENCH_DIR", "/tmp/agb_bench")
SCALES = [int(x) for x in os.environ.get("BENCH_SCALES", "1,10,100").split(",")]
ENGINES = os.environ.get("BENCH_ENGINES", "pandas,duckdb,spark").split(",")
REPEAT = int(os.environ.get("BENCH_REPEAT", "3"))


def prepare(scale):
    """Réplique les tables silver x scale, en CSV et en Parquet, en local (/tmp)."""
    out = f"{WORK}/x{scale}"
    if os.path.exists(f"{out}/done"):
        return out
    os.makedirs(out, exist_ok=True)
    import duckdb
    src = {"synthese": f"{BASE_SPARK}/silver/synthese", "detail": f"{BASE_SPARK}/silver/detail_ingredient"}
    con = duckdb.connect()
    if src["synthese"].startswith("s3a://"):
        src = {k: v.replace("s3a://", "s3://") for k, v in src.items()}
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(f"""CREATE OR REPLACE SECRET s (TYPE s3, KEY_ID '{os.environ["AWS_ACCESS_KEY_ID"]}',
            SECRET '{os.environ["AWS_SECRET_ACCESS_KEY"]}', SESSION_TOKEN '{os.environ.get("AWS_SESSION_TOKEN", "")}',
            ENDPOINT '{os.environ["AWS_S3_ENDPOINT"]}', URL_STYLE 'path', REGION '{os.environ.get("AWS_DEFAULT_REGION", "us-east-1")}')""")
    for t, p in src.items():
        con.execute(f"""CREATE OR REPLACE TABLE {t} AS
            SELECT * REPLACE (code_agb || '_r' || r AS code_agb)
            FROM read_parquet('{p}/*.parquet'), range({scale}) AS rr(r)""")
        con.execute(f"COPY {t} TO '{out}/{t}.csv' (HEADER)")
        con.execute(f"COPY {t} TO '{out}/{t}.parquet' (FORMAT parquet)")
    open(f"{out}/done", "w").close()
    return out


class PeakRSS:
    """Échantillonne toutes les 50 ms la RSS du processus et de ses descendants (JVM Spark incluse)."""
    def __init__(self):
        import threading
        import psutil
        self.proc, self.peak, self.stop = psutil.Process(), 0, False
        self.t = threading.Thread(target=self.run, daemon=True)
        self.t.start()

    def run(self):
        while not self.stop:
            try:
                rss = self.proc.memory_info().rss + sum(c.memory_info().rss for c in self.proc.children(recursive=True))
                self.peak = max(self.peak, rss)
            except Exception:
                pass
            time.sleep(0.05)


def worker(engine, fmt, path):
    mon = PeakRSS()
    t0 = time.perf_counter()
    if engine == "pandas":
        rd = pd.read_csv if fmt == "csv" else pd.read_parquet
        t1 = time.perf_counter()
        s, d = rd(f"{path}/synthese.{fmt}"), rd(f"{path}/detail.{fmt}")
        hot = d.groupby(["groupe", "ingredient_norm"])["cc_kgco2e"].sum().reset_index()
        hot["part"] = hot.cc_kgco2e / hot.groupby("groupe").cc_kgco2e.transform("sum")
        a = d.groupby("code_agb").cc_kgco2e.sum().rename("v").reset_index().merge(s[["code_agb", "cc_kgco2e"]], on="code_agb")
        n = int(((a.v - a.cc_kgco2e).abs() / a.cc_kgco2e > 0.01).sum())
    elif engine == "duckdb":
        import duckdb
        con = duckdb.connect()
        f = "read_csv_auto" if fmt == "csv" else "read_parquet"
        t1 = time.perf_counter()
        con.execute(f"""WITH c AS (SELECT groupe, ingredient_norm, sum(cc_kgco2e) v
                                   FROM {f}('{path}/detail.{fmt}') GROUP BY ALL)
                        SELECT *, v / sum(v) OVER (PARTITION BY groupe) FROM c""").fetchall()
        n = con.execute(f"""WITH a AS (SELECT code_agb, sum(cc_kgco2e) v FROM {f}('{path}/detail.{fmt}') GROUP BY 1)
            SELECT count(*) FILTER (WHERE abs(a.v - s.cc_kgco2e) / s.cc_kgco2e > 0.01)
            FROM a JOIN {f}('{path}/synthese.{fmt}') s USING (code_agb)""").fetchone()[0]
    else:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.master("local[*]").appName("bench").config("spark.sql.shuffle.partitions", "8").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        t1 = time.perf_counter()
        rd = (lambda p: spark.read.csv(p, header=True, inferSchema=True, multiLine=True, escape='"')) if fmt == "csv" else spark.read.parquet
        rd(f"{path}/detail.{fmt}").createOrReplaceTempView("d")
        rd(f"{path}/synthese.{fmt}").createOrReplaceTempView("s")
        spark.sql("""WITH c AS (SELECT groupe, ingredient_norm, sum(cc_kgco2e) v FROM d GROUP BY 1, 2)
                     SELECT *, v / sum(v) OVER (PARTITION BY groupe) FROM c""").collect()
        n = spark.sql("""WITH a AS (SELECT code_agb, sum(cc_kgco2e) v FROM d GROUP BY 1)
            SELECT count_if(abs(a.v - s.cc_kgco2e) / s.cc_kgco2e > 0.01) FROM a JOIN s USING (code_agb)""").first()[0]
    t2 = time.perf_counter()
    time.sleep(0.1)
    print(f"RESULT {t2 - t0:.3f} {t2 - t1:.3f} {n} {mon.peak / 2**20:.1f}")


if __name__ == "__main__":
    if len(sys.argv) == 4:
        worker(*sys.argv[1:])
        sys.exit(0)
    banner("05_benchmark")
    rows = []
    for scale in SCALES:
        path = prepare(scale)
        size_csv = sum(os.path.getsize(f"{path}/{t}.csv") for t in ("synthese", "detail"))
        size_pq = sum(os.path.getsize(f"{path}/{t}.parquet") for t in ("synthese", "detail"))
        for fmt, size in (("csv", size_csv), ("parquet", size_pq)):
            for engine in ENGINES:
                for rep in range(REPEAT):
                    p = subprocess.run([sys.executable, __file__, engine, fmt, path], capture_output=True, text=True, cwd=HERE)
                    line = [l for l in p.stdout.splitlines() if l.startswith("RESULT")]
                    if not line:
                        print(p.stderr[-2000:])
                        continue
                    _, total, compute, n, peak = line[0].split()
                    row = dict(scale=scale, format=fmt, octets=size, moteur=engine, essai=rep + 1,
                               temps_total_s=total, temps_calcul_s=compute,
                               pic_rss_mo=peak,
                               produits_hors_tolerance=n)
                    rows.append(row)
                    print(row, flush=True)
    os.makedirs(os.path.join(HERE, "..", "reports"), exist_ok=True)
    out = os.path.join(HERE, "..", "reports", "benchmark.csv")
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print(f"-> {out}")
