from flask import Flask, request, jsonify
from datetime import datetime
from data_pipeline.minio_client import create_bucket_if_not_exists, upload_file, download_file
from data_pipeline.clickhouse_client import execute_sql_script, get_client, insert_dataframe
from data_pipeline.data_processing import process_data, prepare_dataframe_for_insert
import pandas as pd
import os
import requests

app = Flask(__name__)

# Criar bucket se não existir
create_bucket_if_not_exists("raw-data")

# Executar o script SQL para criar a tabela
execute_sql_script('sql/create_table.sql')


@app.route('/upload_api', methods=['GET'])
def receive_data():
    try:
        response = requests.get('https://api.thecatapi.com/v1/images/search')
        data = response.json()

        # Verifique se a lista contém elementos (pelo menos um gato)
        if data:
            # Pega o primeiro elemento da lista (assumindo que há um gato)
            primeiro_item = data[0]
            # Extrai a URL do dicionário dentro do primeiro elemento
            image_url = primeiro_item['url']
            # Crie o DataFrame com a URL
            df = pd.DataFrame({'image_url': [image_url]})
            client = get_client()  # Obter o cliente ClickHouse
            insert_dataframe(client, 'cat_images', df)
            return jsonify({"message": "Imagem de gato baixada e salva com sucesso"}), 200
        else:
            return jsonify({"error": "A resposta da API não contém a URL da imagem"}), 500
        
    except Exception as e :
        return jsonify({"error":f"Erro interno do servidor: {str(e)}"}, 500)
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
