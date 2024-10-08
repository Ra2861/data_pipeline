## Projeto Demonstrativo de Ingestão de Dados

Este projeto tem como objetivo demonstrar metodologias para a ingestão de dados em plataformas analíticas, utilizando ferramentas como ClickHouse, MinIO e testes integrados para garantir a qualidade e integridade dos dados.

### Pré-requisitos

- **Docker**: Certifique-se de ter o Docker instalado e em funcionamento no seu ambiente.
- **Visual Studio Code (VSCode)**: Editor de código recomendado para desenvolver e testar o projeto.
- **Extensão REST Client (VSCode)**: Extensão para facilitar a execução de requisições HTTP diretamente no VSCode.
- **DBVier**:

### Instruções para Execução

1. **Clonar o Repositório**:
   ```bash
   git clone https://github.com/seu-usuario/seu-repositorio.git
   cd seu-repositorio
   ```

2. **Configuração do Docker Compose**:
   O projeto inclui um arquivo `docker-compose.yml` que define os serviços necessários (MinIO, ClickHouse, etc.). Certifique-se de estar na raiz do projeto, onde o arquivo `docker-compose.yml` está localizado.

3. **Iniciar os Contêineres**:
   Para subir as imagens do MinIO e ClickHouse, e visualizar suas interfaces:
   ```bash
   docker-compose up --build
   ```

   Isso irá:
   - Iniciar o MinIO na porta `9000` (API) e `9001` (UI).
   - Iniciar o ClickHouse na porta `8123` (interface HTTP).

4. **Acessar Interfaces**:
   - **MinIO**: Acesse `http://localhost:9001` no seu navegador para visualizar a interface de usuário do MinIO.
   - Na interface de login do minio no user e senha insira minioadmin
   - **ClickHouse**: Utilize ferramentas como DBVisualizer para conectar-se ao ClickHouse, ou acesse a interface HTTP em `http://localhost:8123`.
   - No visual studio dentro do diretório execute `python .\app.py`

### Testes

Para testar rodar os testes execute `python .\tests\test_app.py`.


### Considerações Finais

Este projeto serve como um ponto de partida para implementar soluções de ingestão de dados robustas e escaláveis. Ele também demonstra boas práticas em termos de organização de código, tratamento de exceções, e testes automatizados.

![Captura de tela 2024-08-28 142005](https://github.com/user-attachments/assets/cf6c0d6d-ac11-4241-8347-b6fcb1697a4c)
não vai dar dez pra esse gatinho fofinho :(
