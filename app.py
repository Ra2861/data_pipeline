from flask import Flask, request, jsonify
from datetime import datetime
from data_pipeline.minio_client import create_bucket_if_not_exists, upload_file, download_file
from data_pipeline.clickhouse_client import execute_sql_script, get_client, insert_dataframe
from data_pipeline.data_processing import process_data, prepare_dataframe_for_insert
import pandas as pd
import os
import requests

# Cria a aplicação Flask
app = Flask(__name__)

# Cria o bucket "raw-data" no MinIO se ele não existir
create_bucket_if_not_exists("raw-data")

# Executa o script SQL para criar a tabela "cat_images" no ClickHouse
execute_sql_script('sql/create_table.sql')


@app.route('/upload_api', methods=['GET'])
def receive_data():

    try:
        # Faz uma requisição GET para a API Cat API
        response = requests.get('https://api.thecatapi.com/v1/images/search')
        data = response.json()  # Converte a resposta para JSON

        # Verifica se a lista de imagens retornada pela API não está vazia
        if data:
            # Pega o primeiro elemento da lista (assumindo que há um gato)
            primeiro_item = data[0]
            # Extrai a URL da imagem do dicionário dentro do primeiro elemento
            image_url = primeiro_item['url']

            # Cria um DataFrame do Pandas com a URL da imagem
            df = pd.DataFrame({'image_url': [image_url]})

            # Obtém o cliente ClickHouse
            client = get_client()

            # Insere o DataFrame na tabela "cat_images" do ClickHouse
            insert_dataframe(client, 'cat_images', df)

            # Retorna uma mensagem de sucesso
            return jsonify({"message": "Imagem de gato baixada e salva com sucesso"}), 200
        else:
            # Retorna um erro se a lista estiver vazia (nenhum gato encontrado)
            return jsonify({"error": "A resposta da API não contém a URL da imagem"}), 500

    except Exception as e:  # Captura qualquer tipo de exceção
        # Retorna um erro genérico com a mensagem da exceção
        return jsonify({"error": f"Erro interno do servidor: {str(e)}"}), 500

if __name__ == '__main__':
    # Inicia a aplicação Flask na porta 5000 e escuta em todas as interfaces
    app.run(host='0.0.0.0', port=5000)
