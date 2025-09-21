# Projeto Saldo de Empenhos

**Descrição**
Este projeto tem como objetivo calcular o saldo de empenho a partir de dados obtidos via API do Portal de Transparência.
Esse projeto foi criado com intenção de resolver uma dor de negocio para empresas que trabalham com funcionarismo publico com valores contratuais fixos e "sobre demanda" e que podem responder rapidamente qual o saldo remanescente do emepenho  e ser utilizado para sabermos qual o saldo atual do empenho que temos atualmente  e ja alertar responsaveis do contrato que o saldo está ficando curto ou até mesmo que nao ha saldo antes mesmo de iniciarmos um novo serviço sob demanda.

## Funcionalidades
- Consulta à API do Portal de Transparência: Obtenção de dados de empenhos através de diversas consultas à API.
- Extração e Processamento de dados: Filtra e organiza informações de empenhos por fase, favorecido e valor.
- Calculo de Saldos dos Empenhos: Determina o valor pago e o saldo a pagar para cada empenho, consolidando os dados para análise.
- Registro de atividades: `logging` para registrar o progresso e possíveis erros durante a execução.


## Instalação e Uso 
1. Clone o repositório em sua maquoinal local:
''' bash
git clone git clone https://github.com/pedrovcgomes/Projeto_Saldo_Empenhos.git
'''

2. Crie um ambiente virual (recomendado):
''' bash
python -m venv Saldo_Empenho
source Saldo_Empenho\Scripts\activate

3. Realizar a instalação das dependências do projeto:
''' bash
pip install -r requirements.txt
'''

4. Execute os script da pasta src para calcular os saldos:

**Estrutura do Projeto** 

Projeto_Saldo_Empenhos/
├── .git/                  # Gerenciado pelo Git (pasta oculta)
├── .venv/                 # Ambiente virtual (ou Saldo_Empenho/)
├── data/
│   ├── raw/               # Dados brutos, exatamente como extraídos
│   │   └── 2023/          # Exemplo de subpasta por ano
│   │       ├── empenhos_2023.csv
│   │       ├── liquidacoes_2023.csv
│   │       └── pagamentos_2023.csv
│   └── processed/         # Dados tratados, limpos e prontos para análise
├── notebooks/             # Jupyter Notebooks para exploração e prototipagem
│   └── 1_extract_raw_data.ipynb  # Notebook teste para coletar e salvar os dados brutos
├── src/                   # Scripts Python para extração, tratamento e análise
│   ├── __init__.py        # Arquivo vazio para indicar que 'src' é um pacote Python
│   ├── extraction.py      # Script para a lógica de extração
│   ├── processing.py      # Script para a lógica de tratamento
│   └── api_client.py      # Script para conexão com API
│   └── calculate_balances.py # Script para a lógica de cálculo de saldo
├── .gitignore             # Define quais arquivos e pastas o Git deve ignorar
├── requirements.txt       # Lista todas as dependências do projeto com suas versões
└── README.md              # Descrição do projeto, instruções, etc.

## Tecnlogias Utilizadas
- Python
- Jupyter Notebook
- Pandas
- logging
- Git


## Contribuição

Este projeto foi desenvolvido por Pedro Vitor de Carvalho Gomes.

## Licença

Este projeto está licenciado sob a Licença MIT.
