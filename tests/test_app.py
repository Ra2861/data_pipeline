import pytest
from app import app

# Fixture para criar um cliente de teste do Flask
@pytest.fixture
def client():
    with app.test_client() as client:
        yield client

# Teste para verificar o sucesso ao receber dados válidos
def test_receive_data_success(client, mocker):
    # Mockando as funções para evitar dependências externas durante o teste
    mocker.patch('data_pipeline.minio_client.create_bucket_if_not_exists')
    mocker.patch('data_pipeline.minio_client.upload_file')
    mocker.patch('data_pipeline.minio_client.download_file')
    mocker.patch('data_pipeline.clickhouse_client.execute_sql_script')
    mocker.patch('data_pipeline.clickhouse_client.get_client')
    mocker.patch('data_pipeline.clickhouse_client.insert_dataframe')
    mocker.patch('data_pipeline.data_processing.process_data', return_value='test.parquet')
    mocker.patch('pandas.read_parquet', return_value='mocked_dataframe')
    mocker.patch('data_pipeline.data_processing.prepare_dataframe_for_insert', return_value='prepared_dataframe')

    # Dados válidos de entrada
    data = {
        "date": 1659422400,  # Timestamp válido
        "dados": 100
    }

    # Envia uma requisição POST com os dados
    response = client.post('/data', json=data)
    
    # Verifica se a resposta foi bem-sucedida e se a mensagem retornada é a esperada
    assert response.status_code == 200
    assert response.json == {"message": "Dados recebidos, armazenados e processados com sucesso"}

# Teste para verificar o comportamento com dados em formato inválido
def test_receive_data_invalid_format(client):
    # Dados com formato inválido
    data = {
        "invalid_field": "invalid_value"
    }

    # Envia uma requisição POST com os dados
    response = client.post('/data', json=data)
    
    # Verifica se a resposta retorna um erro 400 e a mensagem correta
    assert response.status_code == 400
    assert response.json == {"error": "Formato de dados inválido"}

# Teste para verificar o comportamento com tipo de dados inválido
def test_receive_data_invalid_data_type(client):
    # Dados com tipos inválidos
    data = {
        "date": "not_a_timestamp",
        "dados": "not_an_integer"
    }

    # Envia uma requisição POST com os dados
    response = client.post('/data', json=data)
    
    # Verifica se a resposta retorna um erro 400 e a mensagem correta
    assert response.status_code == 400
    assert response.json == {"error": "Tipo de dados inválido"}

# Teste para simular um erro durante o processamento dos dados
def test_receive_data_processing_error(client, mocker):
    # Mockando as funções para simular um erro durante o processamento
    mocker.patch('data_pipeline.minio_client.create_bucket_if_not_exists')
    mocker.patch('data_pipeline.clickhouse_client.execute_sql_script')
    mocker.patch('data_pipeline.data_processing.process_data', side_effect=Exception('Erro de processamento'))

    # Dados válidos de entrada
    data = {
        "date": 1659422400,
        "dados": 100
    }

    # Envia uma requisição POST com os dados
    response = client.post('/data', json=data)
    
    # Verifica se a resposta retorna um erro 500 e a mensagem correta
    assert response.status_code == 500
    assert response.json == {"error": "Erro ao processar e salvar os dados: Erro de processamento"}

# Teste para simular um erro ao inserir dados no ClickHouse
def test_receive_data_clickhouse_error(client, mocker):
    # Mockando as funções para simular um erro durante a inserção no ClickHouse
    mocker.patch('data_pipeline.minio_client.create_bucket_if_not_exists')
    mocker.patch('data_pipeline.clickhouse_client.execute_sql_script')
    mocker.patch('data_pipeline.data_processing.process_data', return_value='test.parquet')
    mocker.patch('data_pipeline.minio_client.upload_file')
    mocker.patch('data_pipeline.minio_client.download_file')
    mocker.patch('pandas.read_parquet', return_value='mocked_dataframe')
    mocker.patch('data_pipeline.data_processing.prepare_dataframe_for_insert', return_value='prepared_dataframe')
    mocker.patch('data_pipeline.clickhouse_client.get_client')
    mocker.patch('data_pipeline.clickhouse_client.insert_dataframe', side_effect=Exception('Erro no ClickHouse'))

    # Dados válidos de entrada
    data = {
        "date": 1659422400,
        "dados": 100
    }

    # Envia uma requisição POST com os dados
    response = client.post('/data', json=data)
    
    # Verifica se a resposta retorna um erro 500 e a mensagem correta
    assert response.status_code == 500
    assert response.json == {"error": "Erro ao preparar ou inserir dados no ClickHouse: Erro no ClickHouse"}
