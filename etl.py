import pandas as pd
import logging
import boto3
from dotenv import load_dotenv
from typing import List
import os


# Carrega as variáveis de ambiente
load_dotenv()

# Configuração do logging para capturar mensagens de nível INFO e acima
logging.basicConfig(
    level=logging.INFO,  # Define o nível mínimo como INFO
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Formato da mensagem
)

# Conffiguração da AWS a partir do .env
AWS_ACCESS_KEY_ID :str = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY :str = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION :str = os.getenv('AWS_REGION')


def get_local_files(path:str) -> List[str]:
    """
     Função que lê a pasta de Dados do computador
    Args:
        param1 (path): caminho da pasta
    Returns:
        tipo: retorna uma lista de arquivos presentes na pasta 
    """
    os.chdir(path)
    os.getcwd()
    try:
        logging.info('Search files...')
        list_files = os.listdir()
        logging.info(f"Folder: {list_files}")
    except Exception as e:
        logging.info(e)
    return list_files


def s3_folders() -> List[str]:
    s3 = boto3.client('s3')
    response = s3.list_buckets()

    list_buckets = []  # Cria uma lista vazia para armazenar os nomes dos buckets
    for bucket in response['Buckets']:
        list_buckets.append(bucket["Name"])  # Adiciona o nome do bucket à lista
        logging.info(f'S3 Bucket : {bucket["Name"]}')

    return list_buckets


def compare_lists(list_files, list_buckets):
    if list_files == list_buckets:
       logging.info("")
    else:
        missing_files = set(list_files) - set(list_buckets)
        logging.info(f"Arquivos faltantes: {missing_files}")
    return missing_files


def create_bucket(bucket_names: List[str], region: str = 'us-east-1'):
    for bucket in bucket_names:
        try:
            if region == 'us-east-1':
                s3_client = boto3.client('s3', region_name=region)
                s3_client.create_bucket(Bucket=bucket)
            else:
                s3_client = boto3.client('s3', region_name=region)
                location = {'LocationConstraint': region}
                s3_client.create_bucket(Bucket=bucket, CreateBucketConfiguration=location)
            
            logging.info(f"Bucket {bucket} criado com sucesso na região {region}")
        
        except Exception as e:
            logging.error(f"Erro ao criar o bucket {bucket}: {e}")
            return False
    
    return True  

arquivos_computador = get_local_files(path=r'C:\Users\milas\Downloads\ESTUDOS\LUCIANO_GALVAO\Dados')
lista_buckets = s3_folders()

comparador = compare_lists(arquivos_computador, lista_buckets)
if comparador:
    create_bucket(comparador,region='us-west-2') 
else:
    logging.info("Nenhum bucket novo precisa ser criado.")
