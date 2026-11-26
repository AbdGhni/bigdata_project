#!/usr/bin/env bash
# Ingestion bronze SANS Kubernetes (repli si le Job k8s/job-ingest-bronze.yaml ne peut pas tourner).
# Télécharge les 2 CSV AGRIBALYSE, contrôle la taille totale [1 Mo ; 5 Mo], calcule les SHA-256,
# puis dépose dans s3://$LAB_BUCKET_NAME/bigdata_project/bronze/. Relançable : un fichier déjà
# présent avec le même SHA-256 n'est pas renvoyé.
set -euo pipefail
BUCKET="${LAB_BUCKET_NAME:-$KUBERNETES_NAMESPACE}"
DEST="s3://$BUCKET/bigdata_project/bronze"
TMP="${TMPDIR:-/tmp}/agb_ingest"; mkdir -p "$TMP"
BASE_URL="https://data.ademe.fr/data-fair/api/v1/datasets"
declare -A SRC=(
  [agribalyse_synthese.csv]="$BASE_URL/agribalyse-31-synthese/raw"
  [agribalyse_detail_ingredient.csv]="$BASE_URL/agribalyse-31-detail-par-ingredient/raw"
)
echo "=== [00_ingest] namespace=${KUBERNETES_NAMESPACE:-?} date=$(date '+%F %T %Z') dest=$DEST"
total=0
for f in "${!SRC[@]}"; do
  curl -fsSL --retry 3 -o "$TMP/$f" "${SRC[$f]}"
  size=$(stat -c %s "$TMP/$f"); total=$((total + size))
  sha=$(sha256sum "$TMP/$f" | cut -d' ' -f1)
  echo "$f  $size octets  sha256=$sha"
  remote_sha=$(aws s3api head-object --bucket "$BUCKET" --key "bigdata_project/bronze/$f" \
               --query 'Metadata.sha256' --output text 2>/dev/null || true)
  if [[ "$remote_sha" == "$sha" ]]; then
    echo "  déjà présent et identique -> non renvoyé"
  else
    aws s3 cp "$TMP/$f" "$DEST/$f" --metadata "sha256=$sha,source=${SRC[$f]},downloaded=$(date -u +%FT%TZ)"
  fi
done
echo "TAILLE TOTALE BRUTE : $total octets ($(awk "BEGIN{printf \"%.3f\", $total/1e6}") Mo)"
if (( total <= 1000000 || total >= 5000000 )); then echo "ERREUR : hors borne [1 Mo ; 5 Mo]"; exit 1; fi
aws s3 ls "$DEST/" --human-readable --summarize
