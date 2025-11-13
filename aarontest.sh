for i in $(seq 1 3); do
  echo
  echo
  echo "Uploading Hospital day ${i} data"
  echo
  curl -X 'POST' \
    'http://0.0.0.0:8001/upload/patient-data' \
    -H 'accept: application/json' \
    -H 'Content-Type: multipart/form-data' \
    -F "file=@./app/dataset/synthetic_data/hospital_1_day_${i}.csv;type=text/csv"
done

echo "Adding Full Access Policy"
echo
curl -X 'POST' \
  'http://0.0.0.0:8001/access-policies' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "wallet_address": "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
  "table_name": "patient_data",
  "policy_sql": "SELECT * FROM patient_data"
}'