import requests
import pandas as pd
import os
import time
from dotenv import load_dotenv
from pathlib import Path
import json

# --- Configurações que são globais para a extração ---
# como o arquivo api_client.py está em src e o ambiente virtual(.env) está na raiz do projeto, então:
caminho_env = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=caminho_env)

API_KEY = os.getenv('PORTAL_TRANSPARENCIA_API_KEY')
if not API_KEY: #segurança para garantir que a chave da API esteja definida
    raise ValueError("A chave da API (PORTAL_TRANSPARENCIA_API_KEY) não está definida. Verifique seu arquivo .env ou variáveis de ambiente.")
else:
    print("✅ Chave da API carregada com sucesso.")

CNPJ_EMPRESA = "03045711000170"  # CNPJ da empresa a ser consultada
ORDENACAO = 4  # Ordenação dos resultados

FASENOMES = {
    1: "Empenhos",
    2: "Liquidações",
    3: "Pagamentos"
}

# --- Funções de Consulta à API ---

def consultar_dados_por_fase(fase: int, ano: int) -> pd.DataFrame:
    """
    Coleta dados de uma fase específica e ano do Portal da Transparência
    usando a API de 'documentos-por-favorecido'.
    Esta função é para extração em massa por favorecido e fase.
    """
    pagina = 1
    resultados = []

    while True:
        url = "https://api.portaldatransparencia.gov.br/api-de-dados/despesas/documentos-por-favorecido"
        params = {
            "codigoPessoa": CNPJ_EMPRESA,
            "fase": fase,
            "ano": ano,
            "pagina": pagina,
            "ordenacaoResultado": ORDENACAO
        }
        headers = {
            "chave-api-dados": API_KEY,
            "accept": "*/*"
        }
        
        try:
            response = requests.get(url, params=params, headers=headers, timeout=15)
            response.raise_for_status()
            
            dados = response.json()
            if not dados:
                break
                
            resultados.extend(dados)
            
            # Se o lote for menor que 500, provavelmente é o último
            if len(dados) < 500:
                break
                
            pagina += 1
            
        except requests.exceptions.HTTPError as e:
            if response.status_code == 400:
                print(f"❌ Nenhum dado encontrado para a fase {fase} do ano {ano}. Verifique os parâmetros.")
            else:
                print(f'Erro HTTP {response.status_code}: {e}')
            break
        except requests.exceptions.RequestException as e:
            print(f"Erro na conexão: {e}")
            break

    # Converter os dados coletados em DataFrame
    return pd.DataFrame(resultados) if resultados else pd.DataFrame()


def consultar_historico_empenho(codigo_empenho: str, sequencial: int = 1, pagina: int = 1) -> list:
    """
    Consulta o histórico de reforços e anulações de um empenho no Portal da Transparência.
    PROPÓSITO: Obter as operações (INCLUSAO, REFORCO, ANULACAO) que definem o valor atualizado do empenho.
    """
    url = 'https://api.portaldatransparencia.gov.br/api-de-dados/despesas/itens-de-empenho/historico'
    headers = {
        'chave-api-dados': API_KEY,
        'accept': '*/*'
    }
    params = {
        'codigoDocumento': codigo_empenho,
        'sequencial': sequencial,
        'pagina': pagina
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        dados = response.json()
        print(f"✅ Histórico do empenho {codigo_empenho} (Seq:{sequencial}, Pag:{pagina}) retornou {len(dados)} registro(s).")
        return dados
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao consultar histórico do empenho {codigo_empenho} (Seq:{sequencial}, Pag:{pagina}): {e}")
        return []

def coletar_itens_empenho_completos(codigo_empenho: str, max_sequenciais: int = 20) -> dict:
    """
    Consulta todos os itens de um empenho (e seus sequenciais) até encontrar falhas consecutivas.
    PROPÓSITO: Agregador para a consulta de histórico, garantindo que pegamos todo o histórico de um empenho.
    """
    resultados = {}
    sequencial = 1
    falhas_consecutivas = 0
    limite_falhas = 3 

    while sequencial <= max_sequenciais:
        dados = consultar_historico_empenho(codigo_empenho, sequencial=sequencial)
        if dados:
            resultados[sequencial] = dados
            falhas_consecutivas = 0
        else:
            falhas_consecutivas += 1
            if falhas_consecutivas >= limite_falhas:
                print(f"🔚 Parando busca por sequenciais após {falhas_consecutivas} vazios consecutivos para {codigo_empenho}.")
                break
        sequencial += 1
        time.sleep(0.3)
    return resultados


def consultar_documentos_relacionados(codigo_empenho: str) -> list:
    """
    Consulta todos os documentos relacionados a um empenho específico (notas de liquidação, ordens bancárias, etc.).
    PROPÓSITO: Obter todos os pagamentos e liquidações associados ao empenho, para calcular o total pago.
    """
    pagina = 1
    resultados = []
    ultimo_lote = None

    while True:
        url = 'https://api.portaldatransparencia.gov.br/api-de-dados/despesas/documentos-relacionados'
        headers = {
            'chave-api-dados': API_KEY,
            'accept': '*/*'
        }
        params = {
            'codigoDocumento': codigo_empenho,
            'fase': 1,  # <--- REINTRODUZINDO FASE=1 AQUI!
            'pagina': pagina,
        }
        try:
            response = requests.get(url, params=params, headers=headers, timeout=15)
            response.raise_for_status()
        except requests.exceptions.RequestException as err:
            print(f"❌ Erro na requisição de documentos relacionados para o empenho {codigo_empenho}: {err}")
            # print(f"Status: {response.status_code}") # Linha útil para depuração
            # print(f"Resposta da API: {response.text}") # Linha útil para depuração
            return [] # Retorna lista vazia para indicar erro e não travar o processo

        dados = response.json()
        if not dados or dados == ultimo_lote:
            break
        ultimo_lote = dados  # Guarda o lote atual para comparar no próximo loop

        resultados.extend(dados)
        print(f"✅ Doc(s) relacionado(s) ao empenho {codigo_empenho}: Página {pagina} com {len(dados)} registro(s).")
        
        if len(dados) < 500: 
            break
        pagina += 1
        time.sleep(0.5)
    return resultados


# --- Bloco de teste (opcional, pode ser removido ou alterado para testes mais específicos) ---
if __name__ == "__main__":
    print("Testando api_client.py diretamente...")
    # Use o mesmo empenho de teste que sabemos que funciona em ambas as APIs
    empenho_teste_valido = '344001342012022NE000223'

    print(f"\n--- Teste de Histórico de Empenho Completo para {empenho_teste_valido} ---")
    historico_completo = coletar_itens_empenho_completos(empenho_teste_valido)
    print(f"Histórico completo para {empenho_teste_valido}: {json.dumps(historico_completo, indent=2)}")

    print(f"\n--- Teste de Documentos Relacionados para {empenho_teste_valido} ---")
    # Agora, esta linha não dará erro 400
    docs_relacionados = consultar_documentos_relacionados(empenho_teste_valido)
    print(f"Documentos relacionados para {empenho_teste_valido}: {json.dumps(docs_relacionados[:2], indent=2)} (primeiros 2)") # Imprime só os 2 primeiros para não encher o console

    print("\n--- Teste de Coleta por Fase (Exemplo) ---")
    # Este teste continua sendo útil para verificar a outra API se você a usa em outro lugar
    # Mas lembre-se que para o cálculo do saldo do empenho, você usará os detalhes de cada empenho,
    # não esta coleta em massa por favorecido.
    try:
        df_empenhos_2024 = consultar_dados_por_fase(1, 2024)
        print(f"Total de empenhos 2024 coletados por fase: {len(df_empenhos_2024)}")
        print(df_empenhos_2024.head())
    except ValueError as e: # Catch the ValueError if API_KEY is not found
        print(f"Erro ao testar coleta por fase: {e}")
