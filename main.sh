EXPORT_DATASET="london_cycles"
EXPORT_BUCKET="beam-bq-json-export"
PROJECT_ID="${EXPORT_BUCKET}"
EXPORT_TABLE="london_cycles_export"

bq --location="EU" mk --dataset $PROJECT_ID:$EXPORT_DATASET

QUERY="$(cat ./export_query.sql)";
bq query --replace -n 0 --nouse_legacy_sql --nouse_cache --nobatch --allow_large_results --quiet \
--destination_table "${EXPORT_DATASET}.${EXPORT_TABLE}" "${QUERY}"

sudo apt update
yes Y | sudo apt install python3-pip
sudo pip3 install virtualenv

#Dataflow requires python 3.7
virtualenv -p python3 venv

source venv/bin/activate
pip3 install apache-beam[gcp]

echo -e "$(date -u) Make valid json export \n------------------"
source $ROOT_DIR/venv/bin/activate
python $SCRIPTS_DIR/valid_json_bq_export_to_mdm.py --project=fnacdarty-products --region=europe-west1 --runner=DataFlow \
--input= \
--staging_location=gs://$EXPORT_BUCKET/test --temp_location gs://$EXPORT_BUCKET/test \
--output=gs://$EXPORT_BUCKET --save_main_session True
# Important to deactivate venv not to run into issues with update_view script that calls does_it_exist_in_big_query function
# that uses python2 with print (syntax error if default python is python3, which is the case within the virtualenv).
deactivate