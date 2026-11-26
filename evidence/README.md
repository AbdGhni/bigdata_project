# Captures d'écran

Chaque capture montre le namespace Onyxia `user-a-ghani-ece` et la date/heure de capture (ligne `### namespace: ... | capture: ...`), en plus des horodatages des traces d'exécution. Captures prises le 26/11/2026 dans le terminal du service vscode-pyspark-708759.

| Fichier | Contenu | Commande |
|---|---|---|
| 01_onyxia_service.png | Service vscode-pyspark (release Helm), pods du namespace, quota, version Spark | `helm list`, `kubectl get statefulset,pods`, `kubectl get resourcequota` |
| 02_s3_bronze.png | Contenu S3 brut : 2 objets, 3 628 911 octets | `aws s3 ls s3://$KUBERNETES_NAMESPACE/bigdata_project/bronze/ --summarize` |
| 03_job_ingest.png | Job d'ingestion Complete + logs (téléchargement, sha256, taille totale) | `kubectl get jobs,pods`, `kubectl logs job/agb-ingest-bronze --all-containers --timestamps` |
| 04_spark_quality.png | Rapport qualité Spark (lignes vides, espaces, valeurs négatives...) | extrait de `logs/*_3_spark.log` |
| 05_spark_silver_gold.png | Traces Spark silver/gold (comptages) + table gold/groupes relue depuis S3 | extrait de `logs/*_3_spark.log` + lecture Parquet |
| 06_s3_processed.png | Contenu S3 silver / gold / reports | `aws s3 ls --recursive --summarize` |
| 07_job_verify.png | Job DuckDB de vérification : 0 écart, `VERIFICATION OK` | `kubectl logs job/agb-verify-duckdb --timestamps` |
| 08_benchmark.png | Médianes du benchmark pandas / DuckDB / Spark | `reports/benchmark.csv` |
| 09_git_log.png | Historique Git du dépôt | `git log` |
