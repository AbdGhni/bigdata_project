#!/usr/bin/env bash
# Orchestration complète, à lancer depuis le terminal du service Onyxia vscode-pyspark,
# à la racine du dépôt :   bash src/run_all.sh            (tout)
#                          bash src/run_all.sh 3          (reprise à partir de l'étape 3)
#                          BENCH=1 bash src/run_all.sh 5  (benchmarks seulement)
# Étapes : 1 config k8s | 2 ingestion bronze (Job k8s) | 3 qualité + silver + gold (Spark)
#          4 vérification indépendante (Job k8s DuckDB) | 5 benchmarks (si BENCH=1) | 6 copie des logs sur S3
# Chaque étape écrit en overwrite : relancer une étape ne crée ni doublon ni effet de bord.
set -euo pipefail
cd "$(dirname "$0")/.."
FROM="${1:-1}"
export LAB_BUCKET_NAME="${LAB_BUCKET_NAME:-$KUBERNETES_NAMESPACE}"
S3="s3://$LAB_BUCKET_NAME/bigdata_project"
TS="$(date +%Y%m%d_%H%M%S)"
mkdir -p logs
run() {  # run <n> <nom> <commande...> : exécute, horodate et journalise
  local n="$1" name="$2"; shift 2
  (( n < FROM )) && { echo "-- étape $n ($name) sautée"; return 0; }
  local log="logs/${TS}_${n}_${name}.log"
  { echo "##### étape $n $name | namespace=$KUBERNETES_NAMESPACE | $(date '+%F %T %Z') | $(hostname)"
    local t0=$SECONDS; "$@"; echo "##### fin étape $n en $((SECONDS - t0)) s | $(date '+%F %T %Z')"
  } 2>&1 | tee "$log"
  return "${PIPESTATUS[0]}"
}
spark_run() { if command -v spark-submit >/dev/null; then spark-submit --master 'local[*]' "$@"; else python "$@"; fi; }

k8s_setup() {
  kubectl auth can-i create jobs
  kubectl create configmap agb-s3-config \
    --from-literal=AWS_S3_ENDPOINT="$AWS_S3_ENDPOINT" \
    --from-literal=AWS_ENDPOINT_URL="https://$AWS_S3_ENDPOINT" \
    --from-literal=AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}" \
    --from-literal=LAB_BUCKET_NAME="$LAB_BUCKET_NAME" \
    --dry-run=client -o yaml | kubectl apply -f -
  # identifiants STS temporaires : jamais affichés, fichier supprimé aussitôt
  ( umask 077; printf 'AWS_ACCESS_KEY_ID=%s\nAWS_SECRET_ACCESS_KEY=%s\nAWS_SESSION_TOKEN=%s\n' \
      "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY" "${AWS_SESSION_TOKEN:-}" > /tmp/agb_s3.env )
  kubectl create secret generic agb-s3-credentials --from-env-file=/tmp/agb_s3.env \
    --dry-run=client -o yaml | kubectl apply -f - ; rm -f /tmp/agb_s3.env
  kubectl create configmap agb-verify-code --from-file=src/common.py --from-file=src/04_verify_duckdb.py \
    --dry-run=client -o yaml | kubectl apply -f -
  kubectl get configmap agb-s3-config agb-verify-code && kubectl get secret agb-s3-credentials
}

k8s_job() {  # k8s_job <manifest> <nom-du-job> : (re)lance le Job, attend, affiche les traces
  kubectl delete job "$2" --ignore-not-found --wait=true
  kubectl apply -f "$1"
  local st=""
  for _ in $(seq 1 120); do
    st="$(kubectl get job "$2" -o jsonpath='{.status.succeeded}/{.status.failed}')"
    [[ "$st" == 1/* || "$st" == */1 ]] && break
    sleep 5
  done
  kubectl get job,pods -l app=bigdata-project -o wide
  kubectl logs "job/$2" --all-containers --timestamps --prefix
  [[ "$st" == 1/* ]] || { echo "JOB $2 EN ECHEC (status=$st)"; return 1; }
}

run 1 k8s_setup    k8s_setup
run 2 ingest       k8s_job k8s/job-ingest-bronze.yaml agb-ingest-bronze
run 3 spark        bash -c "$(declare -f spark_run); set -e
                   spark_run src/01_quality_report.py; spark_run src/02_silver.py; spark_run src/03_gold.py"
run 4 verify       k8s_job k8s/job-verify-duckdb.yaml agb-verify-duckdb
if [[ "${BENCH:-0}" == "1" ]]; then run 5 benchmark python src/05_benchmark.py; fi
run 6 publish_logs bash -c "aws s3 cp logs/ $S3/reports/logs/ --recursive --exclude '*' --include '${TS}_*'
                   [ -f reports/benchmark.csv ] && aws s3 cp reports/benchmark.csv $S3/reports/benchmark.csv || true
                   aws s3 ls $S3/ --recursive --human-readable --summarize"
