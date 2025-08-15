import os
import boto3
import geopandas as gpd
from io import StringIO
from dotenv import load_dotenv

class S3Manager:
    load_dotenv()
    def __init__(self, region="eu-west-3"):
        self.s3 = boto3.client(
            service_name="s3",
            region_name="PAR",
            endpoint_url=os.getenv("S3_ENDPOINT"),
            aws_access_key_id=os.getenv("SCW_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("SCW_SECRET_KEY")
        )

    def list_bucket_contents(self, bucket_name):
        try:
            result = self.s3.list_objects_v2(Bucket=bucket_name)
            if "Contents" in result:
                print("Voici les objets présents dans le bucket :")
                for obj in result["Contents"]:
                    print(f"- {obj['Key']}")
            else:
                print("Le bucket est vide ou inaccessible.")
        except Exception as e:
            print(f"❌ Erreur lors de la lecture du bucket : {e}")

    def load_geojson_from_s3(self, bucket_name, s3_key):
        try:
            response = self.s3.get_object(Bucket=bucket_name, Key=s3_key)
            geojson_data = response['Body'].read().decode('utf-8')

            gtp_commune_m = gpd.read_file(StringIO(geojson_data))

            print("✅ Fichier chargé et converti en GeoDataFrame avec succès")
            return gtp_commune_m
        except Exception as e:
            print(f"❌ Erreur lors du chargement ou de la conversion du fichier GeoJSON: {e}")
            return None

    def download_from_s3(self, bucket_name, s3_key, download_path):
        try:
            with open(download_path, 'wb') as f:
                self.s3.download_fileobj(bucket_name, s3_key, f)
            print(f'✅ Fichier téléchargé depuis S3 : {download_path}')
        except Exception as e:
            print(f"❌ Erreur lors du téléchargement du fichier depuis S3: {e}")

    def delete_from_s3(self, bucket_name, s3_key):
        try:
            self.s3.delete_object(Bucket=bucket_name, Key=s3_key)
            print(f"✅ Fichier supprimé avec succès : {s3_key}")
        except Exception as e:
            print(f"❌ Erreur lors de la suppression du fichier depuis S3: {e}")

    def upload_to_s3(self, bucket_name, file_path, s3_key):
        try:
            with open(file_path, "rb") as f:
                self.s3.upload_fileobj(f, bucket_name, s3_key)
            print(f"✅ Fichier uploadé avec succès : {s3_key}")
        except Exception as e:
            print(f"❌ Erreur lors de l'upload du fichier vers S3: {e}")
