EXPORT_DATASET="london_cycles"
EXPORT_BUCKET="beam-bq-json-export"
PROJECT_ID="${EXPORT_BUCKET}"
EXPORT_TABLE="london_cycles_export"
export GOOGLE_APPLICATION_CREDENTIALS="service_account_key.json"

bq --location="EU" mk --dataset $PROJECT_ID:$EXPORT_DATASET

QUERY="$(cat ./export_query.sql)";
bq query --replace -n 0 --nouse_legacy_sql --nouse_cache --nobatch --allow_large_results --quiet \
--destination_table "${PROJECT_ID}:${EXPORT_DATASET}.${EXPORT_TABLE}" "${QUERY}"

#Dataflow requires python 3.7
virtualenv -p python3 venv

source venv/bin/activate
# Use quotation marks for zsh mac
pip3 install "apache-beam[gcp]"

python ./beam_bq_json_export.py --project="${PROJECT_ID}" --region=europe-west1 --runner=DataFlow \
--input="${PROJECT_ID}:${EXPORT_DATASET}.${EXPORT_TABLE}" \
--staging_location=gs://$EXPORT_BUCKET/test --temp_location gs://$EXPORT_BUCKET/test \
--output=gs://$EXPORT_BUCKET --save_main_session True
deactivate