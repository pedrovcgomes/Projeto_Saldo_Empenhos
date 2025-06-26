import requests
import pandas as pd
import os
import time
# --- Configurações que são globais para a extração ---
# CNPJ_EMPRESA, ORDENACAO, API_KEY e FASENOMES devem vir para cá
CNPJ_EMPRESA = "03045711000170"
ORDENACAO = 4
API_KEY = "***REMOVED_API_KEY_PLACEHOLDER***"

FASENOMES = {
    1: "Empenhos",
    2: "Liquidações",
    3: "Pagamentos"
}
# --- Fim das Configurações ---
def coletar_dados_por_fase(fase: int, ano: int) -> pd.DataFrame:
    """
    Coleta dados de uma fase específica e ano do Portal da Transparência.

    Args:
        fase (int): Código da fase (1 para Empenhos, 2 para Liquidações, 3 para Pagamentos).
        ano (int): Ano da coleta.

    Returns:
        pd.DataFrame: DataFrame contendo os dados coletados.
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
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status() # Lança um HTTPError para 4xx/5xx responses
            dados = response.json()
            
            if not dados:
                break
            
            resultados.extend(dados)
            print(f"✅ {FASENOMES[fase]} {ano}: Página {pagina} com {len(dados)} registros")
            pagina += 1
            time.sleep(0.5) # Boa prática: Pausa para não sobrecarregar a API
            
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Erro de requisição na fase {fase}, ano {ano}, página {pagina}: {e}")
            break
        except ValueError as e:
            print(f"⚠️ Erro ao decodificar JSON na fase {fase}, ano {ano}, página {pagina}: {e}. Resposta: {response.text}")
            break

    return pd.DataFrame(resultados)


def run_extraction(anos_para_coletar: list, base_raw_dir: str):
    """
    Orquestra a coleta e o salvamento de dados para múltiplos anos e fases.

    Args:
        anos_para_coletar (list): Lista de anos para coletar os dados.
        base_raw_dir (str): Caminho base onde os dados brutos serão salvos (ex: "data/raw").
    """
    for ano in anos_para_coletar:
        for fase in [1, 2, 3]:
            print(f"\nIniciando coleta para {FASENOMES[fase]} no ano {ano}...")
            df = coletar_dados_por_fase(fase, ano)
            
            if not df.empty:
                ano_dir = os.path.join(base_raw_dir, str(ano))
                os.makedirs(ano_dir, exist_ok=True)
                
                nome_arquivo_csv = f"{FASENOMES[fase].lower()}_{ano}.csv"
                caminho_completo_arquivo = os.path.join(ano_dir, nome_arquivo_csv)
                
                df.to_csv(caminho_completo_arquivo, index=False, encoding='utf-8-sig')
                print(f"📁 CSV salvo: {caminho_completo_arquivo}")
            else:
                print(f"⚠️ Nenhum dado coletado para {FASENOMES[fase]} no ano {ano}. CSV não salvo.")

# Esta parte só será executada se o script for rodado diretamente (não importado)
if __name__ == "__main__":
    ANOS_COLETA = [2023, 2024, 2025] # Defina aqui os anos que deseja coletar ao rodar o script diretamente
    BASE_RAW_DATA_PATH = "data/raw" # Caminho relativo à raiz do projeto

    print("Iniciando processo de extração de dados brutos...")
    run_extraction(ANOS_COLETA, BASE_RAW_DATA_PATH)
    print("\nProcesso de extração concluído!")