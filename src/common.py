"""Paramètres partagés du pipeline AGRIBALYSE (chemins S3, session Spark, renommage des colonnes)."""
import os
import unicodedata

# Bucket Onyxia = namespace (ex. user-a-ghani-ece). En local, on écrit dans ./local_lake
BUCKET = os.environ.get("LAB_BUCKET_NAME") or os.environ.get("KUBERNETES_NAMESPACE")
PREFIX = "bigdata_project"
ON_S3 = bool(BUCKET and os.environ.get("AWS_S3_ENDPOINT"))

if ON_S3:
    BASE_SPARK = f"s3a://{BUCKET}/{PREFIX}"
    BASE_DUCK = f"s3://{BUCKET}/{PREFIX}"
else:
    _repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    _local = os.path.abspath(os.environ.get("LOCAL_LAKE", os.path.join(_repo, "local_lake")))
    BASE_SPARK = BASE_DUCK = _local

BRONZE_FILES = {
    "synthese": "bronze/agribalyse_synthese.csv",
    "detail": "bronze/agribalyse_detail_ingredient.csv",
}

TOLERANCE_REL = 0.01  # écart relatif toléré entre somme des ingrédients et total officiel


def clean_name(col: str) -> str:
    """Normalise un nom de colonne : espaces insécables, espaces doubles, espaces de fin."""
    col = col.replace("\xa0", " ")
    col = " ".join(col.split())
    return col


IMPACTS = {
    "Score unique EF": "score_ef",
    "Changement climatique": "cc_kgco2e",
    "Appauvrissement de la couche d'ozone": "ozone",
    "Rayonnements ionisants": "rayonnements",
    "Formation photochimique d'ozone": "ozone_photochimique",
    "Particules fines": "particules",
    "Effets toxicologiques sur la santé humaine : substances non-cancérogènes": "tox_non_cancer",
    "Effets toxicologiques sur la santé humaine : substances cancérogènes": "tox_cancer",
    "Acidification terrestre et eaux douces": "acidification",
    "Eutrophisation eaux douces": "eutro_eau_douce",
    "Eutrophisation marine": "eutro_marine",
    "Eutrophisation terrestre": "eutro_terrestre",
    "Écotoxicité pour écosystèmes aquatiques d'eau douce": "ecotox",
    "Utilisation du sol": "usage_sol",
    "Épuisement des ressources eau": "res_eau",
    "Épuisement des ressources énergétiques": "res_energie",
    "Épuisement des ressources minéraux": "res_mineraux",
}

RENAME_SYNTHESE = {
    "Code AGB": "code_agb",
    "Code CIQUAL": "code_ciqual",
    "Groupe d'aliment": "groupe",
    "Sous-groupe d'aliment": "sous_groupe",
    "Nom du Produit en Français": "nom_produit",
    "LCI Name": "lci_name",
    "code saison": "code_saison",
    "code avion": "code_avion",
    "Livraison": "livraison",
    "Approche emballage": "emballage",
    "Préparation": "preparation",
    "DQR": "dqr",
    "Changement climatique - émissions biogéniques": "cc_biogenique",
    "Changement climatique - émissions fossiles": "cc_fossile",
    "Changement climatique - émissions liées au changement d'affectation des sols": "cc_affectation_sols",
    **IMPACTS,
}

RENAME_DETAIL = {
    "Ciqual AGB": "code_agb",
    "Ciqual code": "code_ciqual",
    "Nom Français": "nom_produit",
    "Groupe d'aliment": "groupe",
    "Sous-groupe d'aliment": "sous_groupe",
    "LCI Name": "lci_name",
    "Ingredients": "ingredient",
    **IMPACTS,
}


def spark_session(app: str):
    from pyspark.sql import SparkSession

    b = SparkSession.builder.appName(app).master(os.environ.get("SPARK_MASTER", "local[*]"))
    if ON_S3:
        b = (
            b.config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
            .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
            .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
            .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
            )
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )
    b = b.config("spark.sql.session.timeZone", "Europe/Paris").config("spark.sql.shuffle.partitions", "8")
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def banner(step: str):
    """Ligne d'en-tête pour les traces d'exécution (preuves : namespace + date/heure)."""
    import datetime
    import socket

    ns = os.environ.get("KUBERNETES_NAMESPACE", "local")
    now = datetime.datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    print(f"=== [{step}] namespace={ns} host={socket.gethostname()} date={now} base={BASE_SPARK}", flush=True)


def strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
