# bigdata_project : empreinte carbone des aliments par ingrédient (AGRIBALYSE 3.2)

## Auteurs
GHANI Abdullah ; LIN Victor (ECE, Écosystème Big Data II)

## Prérequis
- Compte Onyxia, service `vscode-pyspark` (utilisé : 3 CPU / 3 Gi, quota du namespace), rôle Kubernetes permettant `kubectl create job` dans son namespace.
- Dans le service : `git`, `aws` CLI, `kubectl`, `spark-submit` (fournis par l'image) ; `pip install -r src/requirements.txt`.
- Accès Internet sortant vers `data.ademe.fr` (téléchargement des données brutes).

## Dataset
| Fichier | URL | Licence | Téléchargé le | Taille |
|---|---|---|---|---|
| AGRIBALYSE 3.2 - Synthèse | https://data.ademe.fr/data-fair/api/v1/datasets/agribalyse-31-synthese/raw | Licence Ouverte 2.0 (ADEME) | 2026-11-26 | 1 170 508 octets |
| AGRIBALYSE 3.2 - Détail par ingrédient | https://data.ademe.fr/data-fair/api/v1/datasets/agribalyse-31-detail-par-ingredient/raw | Licence Ouverte 2.0 (ADEME) | 2026-11-26 | 2 458 403 octets |

Total brut : **3 628 911 octets (3,63 Mo)**, dans la borne [1 Mo ; 5 Mo]. Pages : data.gouv.fr/datasets/agribalyse-synthese et data.gouv.fr/datasets/agribalyse-detail-par-ingredient.

## Problématique
Pour chacun des 11 groupes d'aliments d'AGRIBALYSE, quels ingrédients concentrent l'empreinte carbone (kg CO2e/kg de produit) et quelle part revient aux « autres étapes » non rattachées à un ingrédient ? Le pipeline produit, par groupe, la médiane d'empreinte, le classement des ingrédients contributeurs et la part des étapes hors ingrédients. Le résultat est vérifié en contrôlant que, pour les 1 099 produits détaillés, la somme des contributions par ingrédient retrouve le total officiel à 1 % près.

## Chemins S3
Bucket : `user-a-ghani-ece` (= namespace Onyxia). Préfixe : `bigdata_project/`
- Brut : `bronze/agribalyse_synthese.csv`, `bronze/agribalyse_detail_ingredient.csv`
- Nettoyé : `silver/synthese/`, `silver/detail_ingredient/` (Parquet)
- Final : `gold/groupes/`, `gold/ingredients_points_chauds/`, `gold/top_produits/`, `gold/reconciliation/` (Parquet)
- Contrôles : `reports/quality_report/`, `reports/verification_duckdb.json`, `reports/benchmark.csv`, `reports/logs/`

## Procédure de reproduction
```bash
# dans le terminal du service vscode-pyspark
git clone https://github.com/<compte>/bigdata_project.git && cd bigdata_project
pip install -r src/requirements.txt
bash src/run_all.sh            # étapes 1 à 6 (sans benchmarks)
BENCH=1 bash src/run_all.sh 5  # benchmarks pandas / DuckDB / Spark, x1 x10 x100 (~10 min)
```
Étapes : 1 ConfigMaps + Secret k8s | 2 Job `agb-ingest-bronze` (téléchargement -> S3 bronze) | 3 Spark : `01_quality_report.py`, `02_silver.py`, `03_gold.py` | 4 Job `agb-verify-duckdb` (vérification indépendante, sortie `VERIFICATION OK`) | 5 benchmarks | 6 copie des logs sur S3.

Reprise : `bash src/run_all.sh <n>` relance à partir de l'étape n. Toutes les écritures sont en overwrite et les Jobs sont supprimés puis recréés : une relance ne crée pas de doublons. Les identifiants S3 Onyxia sont temporaires : après redémarrage du service, relancer l'étape 1 pour régénérer le Secret.
Sans Kubernetes : `bash src/00_ingest.sh` remplace l'étape 2 et `python src/04_verify_duckdb.py` l'étape 4. Traces horodatées : `logs/`.
