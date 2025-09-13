import requests
import pandas as pd
import os
import time
from dotenv import load_dotenv
from pathlib import Path
import json
from decimal import Decimal

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

# Consulta de empenhos impactados
def consultar_empenho_impactado(codigo_documento, fase, empenho_alvo):
    """
    Consulta a API para descobrir quanto do documento foi usado no empenho específico.
    """
    url = "https://api.portaldatransparencia.gov.br/api-de-dados/despesas/empenhos-impactados"
    
    params = {
        'codigoDocumento': codigo_documento,
        'fase': fase,
        'pagina': 1
    }
    
    headers = {
        'accept': '*/*',
        'chave-api-dados': API_KEY
    }
    
    try:
        print(f"   🔍 Consultando: {codigo_documento} (fase {fase})")
        
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        
        print(f"   📊 Resposta API: {len(data)} empenhos impactados")
        
        # Procurar pelo empenho específico nos resultados
        valor_total = Decimal('0')
        encontrado = False
        
        for item in data:
            if item.get('empenho') == empenho_alvo:
                encontrado = True
                print(f"   ✅ Empenho {empenho_alvo} ENCONTRADO!")
                
                # Verificar campos de valor baseado na fase
                if fase == 3:  # Pagamento
                    campos = ['valorPago', 'valorRestoPago']
                elif fase == 2:  # Liquidação
                    campos = ['valorLiquidado']
                else:
                    campos = []
                
                for campo in campos:
                    valor_str = item.get(campo, '0,00')
                    if valor_str and valor_str != '0,00':
                        valor = Decimal(valor_str.replace('.', '').replace(',', '.'))
                        valor_total += valor
                        print(f"   💰 {campo}: R$ {formatar_valor(valor)}")
                
                break  # Já encontramos, pode sair do loop
        
        if not encontrado:
            print(f"   ❌ Empenho {empenho_alvo} NÃO encontrado nos impactados")
        else:
            print(f"   🎯 Total utilizado: R$ {formatar_valor(valor_total)}")
        
        return valor_total
        
    except requests.RequestException as e:
        error_msg = f"Erro na requisição HTTP: {e}"
        print(f"   ❌ {error_msg}")
        raise Exception(error_msg)
    except json.JSONDecodeError as e:
        error_msg = f"Erro ao decodificar JSON: {e}"
        print(f"   ❌ {error_msg}")
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"Erro inesperado: {e}"
        print(f"   ❌ {error_msg}")
        raise Exception(error_msg)

def formatar_valor(valor):
    """Formata valor decimal para padrão brasileiro"""
    if isinstance(valor, str):
        valor = Decimal(valor.replace('.', '').replace(',', '.'))
    
    valor_str = f"{valor:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
    return valor_str

# --- Bloco de teste (opcional, pode ser removido ou alterado para testes mais específicos) ---
if __name__ == "__main__":
    print("Testando api_client.py diretamente...")
    # Use o mesmo empenho de teste que sabemos que funciona em ambas as APIs
    empenho_teste_valido = '170116000012024NE000060'

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

    #  teste para a função de impacto
    print("\n--- Teste de Empenho Impactado (Exemplo) ---")
    documento_teste = "170116000012023DF803291 " # Documento de pagamento
    fase_teste = 3 # Pagamento
    empenho_alvo_teste = "170116000012023NE000049"
    
    try:
        valor_usado = consultar_empenho_impactado(documento_teste, fase_teste, empenho_alvo_teste)
        print(f"\nValor final usado no empenho {empenho_alvo_teste} pelo documento {documento_teste} foi de R$ {valor_usado:.2f}".replace('.',','))
    except Exception as e:
        print(f"Falha no teste de empenho impactado: {e}")
