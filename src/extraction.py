
import pandas as pd
import os
from dotenv import load_dotenv
from pathlib import Path
import time
import json
from api_client import coletar_itens_empenho_completos, consultar_documentos_relacionados 

# Carregar variáveis de ambiente
load_dotenv()

# Caminho para a lista de empenhos que você quer processar
# AJUSTE ESTE CAMINHO PARA O SEU ARQUIVO REAL DE EMPENHOS
CAMINHO_LISTA_EMPENHOS = Path(__file__).resolve().parent.parent / 'data' / 'trusted' / '2024' / 'empenhos' / 'empenhos_2024_processadas.xlsx'
BASE_RAW_DATA_OUTPUT_DIR = Path(__file__).resolve().parent.parent / 'data' / 'raw_details'

def run_detailed_extraction(caminho_empenhos_input: Path, output_dir: Path):
    """
    Extrai detalhes (histórico e pagamentos) para cada empenho de uma lista.
    """
    os.makedirs(output_dir / 'historicos', exist_ok=True)
    os.makedirs(output_dir / 'pagamentos', exist_ok=True)

    try:
        # Garante que o 'documento' (código do empenho) seja lido como string
        df_empenhos = pd.read_excel(caminho_empenhos_input, dtype={'documento': str})
        print(f"✅ {len(df_empenhos)} empenhos carregados da lista.")
    except FileNotFoundError:
        print(f"❌ Erro: Lista de empenhos não encontrada em '{caminho_empenhos_input}'.")
        return
    except Exception as e:
        print(f"❌ Erro ao carregar a lista de empenhos do Excel: {e}")
        return

    for index, row in df_empenhos.iterrows():
        codigo_empenho = row['documento']
        print(f"\n--- Extraindo detalhes para o empenho: {codigo_empenho} ({index+1}/{len(df_empenhos)}) ---")

        # 1. Extrair Histórico de Itens (para calcular o valor atualizado do empenho)
        # O resultado de coletar_itens_empenho_completos já é um dicionário, pronto para JSON
        historico_itens = coletar_itens_empenho_completos(codigo_empenho)
        if historico_itens: # Salva apenas se houver dados
            with open(output_dir / 'historicos' / f'{codigo_empenho}_historico.json', 'w', encoding='utf-8') as f:
                json.dump(historico_itens, f, indent=4, ensure_ascii=False)
            print(f"✅ Histórico salvo para {codigo_empenho}")
        else:
            print(f"❌ Nenhum histórico encontrado ou erro na extração para {codigo_empenho}. Não será salvo.")

        # 2. Extrair Documentos Relacionados (para obter os pagamentos)
    
        docs_relacionados = consultar_documentos_relacionados(codigo_empenho)
        if docs_relacionados:
            with open(output_dir / 'pagamentos' / f'{codigo_empenho}_documentos_relacionados.json', 'w', encoding='utf-8') as f:
                json.dump(docs_relacionados, f, indent=4, ensure_ascii=False)
            print(f"✅ Documentos relacionados salvos para {codigo_empenho}")
        else:
            print(f"❌ Nenhum documento relacionado encontrado ou erro na extração para {codigo_empenho}. Não será salvo.")
        # Pausa para evitar sobrecarga na API
        time.sleep(1) # Pausa para não sobrecarregar a API

if __name__ == "__main__":
    print("Iniciando a extração detalhada de empenhos...")
    run_detailed_extraction(CAMINHO_LISTA_EMPENHOS, BASE_RAW_DATA_OUTPUT_DIR)
    print("\nExtração detalhada concluída!")