import pytest
from unittest.mock import patch
from your_module import receive_data, get_client, insert_dataframe  # Importe os módulos necessários

@pytest.fixture
def client():
    # Crie um cliente ClickHouse mockado para os testes
    client = MagicMock()
    return client

def test_receive_data_success(client):
    # Simule uma resposta bem-sucedida da API
    with patch('requests.get') as mock_get:
        mock_get.return_value.json.return_value = [{'url': 'https://example.com/image.jpg'}]
        response = receive_data()

        assert response[0].status_code == 200
        assert response[1] == 'Imagem de gato baixada e salva com sucesso'
        client.insert_dataframe.assert_called_once_with('cat_images', pd.DataFrame({'image_url': ['https://example.com/image.jpg']}))

def test_receive_data_empty_list(client):
    # Simula uma resposta da API com uma lista vazia
    with patch('requests.get') as mock_get:
        mock_get.return_value.json.return_value = []
        response = receive_data()

        assert response[0].status_code == 500
        assert response[1]['error'] == 'A resposta da API não contém a URL da imagem'

def test_receive_data_network_error(client):
    # Simula um erro de rede
    with patch('requests.get') as mock_get:
        mock_get.side_effect = requests.exceptions.RequestException('Erro de rede')
        response = receive_data()

        assert response[0].status_code == 500
        assert 'Erro de rede' in response[1]['error']

def test_receive_data_clickhouse_error(client):
    # Simula um erro ao inserir dados no ClickHouse
    with patch('requests.get') as mock_get:
        mock_get.return_value.json.return_value = [{'url': 'https://example.com/image.jpg'}]
        with patch('your_module.insert_dataframe') as mock_insert:
            mock_insert.side_effect = Exception('Erro ao inserir dados')
            response = receive_data()

            assert response[0].status_code == 500
            assert 'Erro ao inserir dados' in response[1]['error']
