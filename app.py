from flask import Flask, request, jsonify
from datetime import datetime
from data_pipeline.minio_client import create_bucket_if_not_exists, upload_file, download_file
from data_pipeline.clickhouse_client import execute_sql_script, get_client, insert_dataframe
from data_pipeline.data_processing import process_data, prepare_dataframe_for_insert
import pandas as pd
import os

app = Flask(__name__)

# Criar bucket se não existir
create_bucket_if_not_exists("raw-data")

# Executar o script SQL para criar a tabela
execute_sql_script('sql/create_table.sql')

@app.route('/data', methods=['POST'])
def receive_data():
    try:
        data = request.get_json()
        if not data or 'date' not in data or 'dados' not in data:
            return jsonify({"error": "Formato de dados inválido"}), 400

        try:
            datetime.fromtimestamp(data['date'])
            int(data['dados'])
        except (ValueError, TypeError):
            return jsonify({"error": "Tipo de dados inválido"}), 400

        # Processar e salvar dados
        try:
            filename = process_data(data)
            upload_file("raw-data", filename)
        except Exception as e:
            return jsonify({"error": f"Erro ao processar e salvar os dados: {str(e)}"}), 500

        # Ler arquivo Parquet do MinIO
        try:
            download_file("raw-data", filename, f"downloaded_{filename}")
            df_parquet = pd.read_parquet(f"downloaded_{filename}")
        except Exception as e:
            return jsonify({"error": f"Erro ao ler o arquivo Parquet: {str(e)}"}), 500

        # Preparar e inserir dados no ClickHouse
        try:
            df_prepared = prepare_dataframe_for_insert(df_parquet)
            client = get_client()  # Obter o cliente ClickHouse
            insert_dataframe(client, 'working_data', df_prepared)
        except Exception as e:
            return jsonify({"error": f"Erro ao preparar ou inserir dados no ClickHouse: {str(e)}"}), 500

        return jsonify({"message": "Dados recebidos, armazenados e processados com sucesso"}), 200
    
    except Exception as e:
        return jsonify({"error": f"Erro interno do servidor: {str(e)}"}), 500

  
@app.route('/upload_csv', methods=['POST'])
    try:
    Verifica se um arquivo foi enviado com a requisição
        if 'file' not in request.files:
            return jsonify({"error": "Nenhum arquivo foi enviado"}), 400
            
        file = request.files['file']
        
        # Verifica se o arquivo é um CSV
        if not file.filename.endswith('.csv'):
            return jsonify({"error": "Formato de arquivo inválido, deve ser um CSV"}), 400
        
        # Carregar o CSV diretamente da memória
        try:
            df_csv = pd.read_csv(file)  
        except Exception as e:
            return jsonify({"error": f"Erro ao ler o arquivo CSV: {str(e)}"}), 500
        
        # Processar e salvar dados no MinIO como Parquet
        try:
            filename = process_data(df_csv)
            upload_file("raw-data", filename)
        except Exception as e:
            return jsonify({"error": f"Erro ao processar e salvar os dados: {str(e)}"}), 500
        
        # Ler arquivo Parquet do MinIO
        try:
            download_file("raw-data", filename, f"downloaded_{filename}")
            df_parquet = pd.read_parquet(f"downloaded_{filename}")
        except Exception as e:
            return jsonify({"error": f"Erro ao ler o arquivo Parquet: {str(e)}"}), 500
        
        # Preparar e inserir dados no ClickHouse
        try:
            df_prepared = prepare_dataframe_for_insert(df_parquet)
            client = get_client()  # Obter o cliente ClickHouse
            insert_dataframe(client, 'working_data', df_prepared)
        except Exception as e:
            return jsonify({"error": f"Erro ao preparar ou inserir dados no ClickHouse: {str(e)}"}), 500
        
        return jsonify({"message": "Arquivo CSV processado e inserido com sucesso"}), 200
    
    except Exception as e:
        return jsonify({"error": f"Erro interno do servidor: {str(e)}"}), 500  
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)