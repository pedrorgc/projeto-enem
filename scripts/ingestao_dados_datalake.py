from google.cloud import storage

client = storage.Client.from_service_account_json('chave/enem-ifnmg-pedrorgc-raipb-da426803e45c.json')

bucket_name = 'enem-bucket-pedrorgc-raipb'
file_path = 'dados/microdados_enem_2023/DADOS/MICRODADOS_ENEM_2023.csv'
destination_blob_name = 'bronze/microdados_enem.csv' 

bucket = client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)

blob.upload_from_filename(file_path)

print(f"Arquivo enviado para gs://{bucket_name}/{destination_blob_name}")
