# src/processing.py

import pandas as pd
import numpy as np
import re
from datetime import datetime
import os

# --- Funções de Limpeza e Conversão ---

def limpar_texto(texto: str) -> str:
    """
    Limpa strings removendo espaços extras e quebras de linha.

    Args:
        texto (str): O texto a ser limpo.

    Returns:
        str: O texto limpo.
    """
    if isinstance(texto, str):
        # Substitui múltiplos espaços e quebras de linha por um único espaço e remove espaços no início/fim
        texto = re.sub(r'\s+', ' ', texto).strip()
    return texto

def limpar_valor(valor: str) -> float:
    """
    Limpa e converte strings de valores monetários para float.
    Lida com "R$", pontos de milhar, vírgulas decimais e sinais negativos.

    Args:
        valor (str): O valor monetário como string.

    Returns:
        float: O valor numérico ou NaN se a conversão falhar.
    """
    if isinstance(valor, str):
        valor = valor.replace("R$", "").replace(".", "").replace(",", ".").strip()
        # Corrige casos como "- 75.88" para "-75.88"
        valor = re.sub(r"^-?\s+", "-", valor)
    try:
        return float(valor)
    except (ValueError, TypeError):
        return np.nan

def eh_ajuste_negativo(observacao: str, valor: float) -> bool:
    """
    Verifica se uma despesa é um ajuste negativo baseado na observação ou valor.

    Args:
        observacao (str): A string de observação da despesa.
        valor (float): O valor numérico da despesa.

    Returns:
        bool: True se for um ajuste negativo, False caso contrário.
    """
    # Converter observação para minúsculas para padronizar a busca
    if isinstance(observacao, str):
        observacao_lower = observacao.lower()
        palavras_chave = ['anulacao', 'reforco', 'cancelamento', 'estorno']
        for palavra in palavras_chave:
            if palavra in observacao_lower:
                return True
    
    # Considerar valores negativos explícitos
    if isinstance(valor, (int, float)) and valor < 0:
        return True
        
    return False

# --- Função Principal de Processamento ---

def processar_dados_financeiros(ano: int, tipo_dado: str, input_base_path: str = 'data', output_base_path: str = 'data'):
    """
    Função principal para processar dados financeiros (pagamentos, empenhos, liquidações).
    Realiza limpeza, conversão de tipos, detecção de duplicatas e ajustes negativos,
    e exporta os dados tratados para a camada trusted.

    Args:
        ano (int): O ano dos dados a serem processados.
        tipo_dado (str): O tipo de dado (ex: 'pagamentos', 'empenhos', 'liquidacoes').
        input_base_path (str): Caminho base para os dados de entrada (raw).
        output_base_path (str): Caminho base para os dados de saída (trusted).

    Returns:
        tuple: Uma tupla contendo (DataFrame completo, DataFrame normal, DataFrame de ajustes negativos).
    """
    # Construção dos caminhos de arquivo de forma flexível
    raw_file_name = f"{tipo_dado}_{ano}.csv"
    caminho_arquivo_raw = os.path.join(input_base_path,'raw', str(ano),  raw_file_name)
    
    trusted_output_dir = os.path.join(output_base_path,  'trusted', str(ano), tipo_dado)
    os.makedirs(trusted_output_dir, exist_ok=True)

    print(f"🔍 Lendo arquivo: {caminho_arquivo_raw}")
    
    try:
        df = pd.read_csv(caminho_arquivo_raw, sep=',', encoding='utf-8')
        print(f"✅ {len(df)} registros carregados de '{raw_file_name}'.")
    except FileNotFoundError:
        print(f"❌ Erro: Arquivo não encontrado em '{caminho_arquivo_raw}'.")
        return None, None, None
    except Exception as e:
        print(f"❌ Erro ao ler o arquivo: {e}")
        return None, None, None
    
    # 1. Limpeza inicial de texto
    # Seleciona colunas de texto (object) e aplica a limpeza
    col_texto = df.select_dtypes(include='object').columns
    for col in col_texto:
        df[col] = df[col].astype(str).apply(limpar_texto)
    print("🧹 Limpeza de texto aplicada.")

    # 2. Conversão de colunas de data
    # Assumimos que a coluna de data se chama 'data'. Ajuste se for diferente.
    if 'data' in df.columns:
        df['data'] = pd.to_datetime(df['data'], format='%d/%m/%Y', errors='coerce')
        print("📅 Coluna 'data' convertida para datetime.")
    else:
        print("⚠️ Coluna 'data' não encontrada. Pulando conversão de data.")
    
    # 3. Conversão da coluna 'valor' para float
    if 'valor' in df.columns:
        df['valor'] = df['valor'].apply(limpar_valor)
        print("💲 Coluna 'valor' convertida para float.")
    else:
        print("⚠️ Coluna 'valor' não encontrada. Pulando conversão de valor.")

    # 4. Criar colunas temporais (se a coluna 'data' existir e for datetime)
    if 'data' in df.columns and pd.api.types.is_datetime64_any_dtype(df['data']):
        df['dia'] = df['data'].dt.day
        df['mes'] = df['data'].dt.month
        df['ano'] = df['data'].dt.year
        df['mes_ano'] = df['data'].dt.strftime('%m-%Y')
        print("📊 Colunas temporais (dia, mês, ano, mês_ano) criadas.")
    else:
        print("⚠️ Não foi possível criar colunas temporais (coluna 'data' ausente ou inválida).")

    # 5. Identificar e remover duplicatas
    # Subconjunto de colunas para identificar duplicatas
    # Ajuste 'documento' se o nome da coluna for diferente para Empenhos/Liquidações
    subset_duplicatas = ['documento', 'valor'] 
    if 'documento' not in df.columns: # Caso o nome da coluna seja 'numero_documento' ou similar
         print("⚠️ Coluna 'documento' não encontrada. Usando ['valor', 'data'] para duplicatas.")
         subset_duplicatas = ['valor', 'data'] # Fallback mais seguro
    
    duplicados_df = pd.DataFrame() # DataFrame vazio para armazenar duplicatas
    
    # O `keep=False` marca todas as ocorrências de duplicatas
    duplicados_marcados = df[df.duplicated(subset=subset_duplicatas, keep=False)]
    
    if not duplicados_marcados.empty:
        print(f"⚠️ Encontrados {len(duplicados_marcados)} registros duplicados com base em {subset_duplicatas}.")
        duplicados_df = duplicados_marcados.copy() # Copia os registros duplicados ANTES de remover
        
        # Remove as duplicatas do DataFrame principal, mantendo a primeira ocorrência
        df = df.drop_duplicates(subset=subset_duplicatas, keep='first').copy()
        
        print("✅ Duplicatas removidas do DataFrame principal.")
    else:
        print("✅ Nenhuma duplicata encontrada.")

    # 6. Detectar ajustes negativos
    # Assumimos que a coluna de observação é 'observacao'. Ajuste se for diferente.
    if 'observacao' in df.columns:
        df['observacao_lower'] = df['observacao'].str.lower() # Cria uma nova coluna temporária para evitar modificar a original se não for desejado
        df['eh_ajuste_negativo'] = df.apply(lambda row: eh_ajuste_negativo(row['observacao_lower'], row['valor']), axis=1)
        df = df.drop(columns=['observacao_lower']) # Remove a coluna temporária
        print("🔎 Ajustes negativos identificados.")
    else:
        print("⚠️ Coluna 'observacao' não encontrada. Pulando detecção de ajustes negativos.")
        df['eh_ajuste_negativo'] = False # Define como False por padrão se não houver observação

    # 7. Separar DataFrames
    registros_normais = df[df['eh_ajuste_negativo'] == False].copy()
    ajustes_negativos = df[df['eh_ajuste_negativo'] == True].copy()
    
    print(f"📁 Registros normais de {tipo_dado}: {len(registros_normais)} | Ajustes negativos de {tipo_dado}: {len(ajustes_negativos)}")

    # 8. Exportar para CSV e Excel
    # Salvando em CSV
    registros_normais.to_csv(os.path.join(trusted_output_dir, f'{tipo_dado}_normais.csv'), index=False, encoding='utf-8-sig')
    ajustes_negativos.to_csv(os.path.join(trusted_output_dir, f'{tipo_dado}_ajustes_negativos.csv'), index=False, encoding='utf-8-sig')
    if not duplicados_df.empty: # Exporta duplicatas apenas se existirem
        duplicados_df.to_csv(os.path.join(trusted_output_dir, f'{tipo_dado}_duplicatas_detectadas.csv'), index=False, encoding='utf-8-sig')


    # Salvando em Excel com múltiplas abas
    output_excel_file = os.path.join(trusted_output_dir, f'{tipo_dado}_{ano}_processadas.xlsx')
    with pd.ExcelWriter(output_excel_file, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Todos_Registros', index=False, float_format="%.2f")
        registros_normais.to_excel(writer, sheet_name=f'{tipo_dado.capitalize()}_Normais', index=False, float_format="%.2f")
        ajustes_negativos.to_excel(writer, sheet_name='Ajustes_Negativos', index=False, float_format="%.2f")
        if not duplicados_df.empty:
            duplicados_df.to_excel(writer, sheet_name='Duplicatas', index=False, float_format="%.2f")

    print(f"✅ Arquivos tratados de {tipo_dado} salvos com sucesso em: {trusted_output_dir}")

    return df, registros_normais, ajustes_negativos