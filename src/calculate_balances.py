import requests
import pandas as pd
import time
import os
import numpy as np
from dotenv import load_dotenv
from pathlib import Path

# --- Configurações ---
load_dotenv()
API_KEY = os.getenv('PORTAL_TRANSPARENCIA_API_KEY')
if not API_KEY:
    raise ValueError("A chave da API (PORTAL_TRANSPARENCIA_API_KEY) não está definida. Verifique seu arquivo .env ou variáveis de ambiente.")

# O caminho de entrada agora é flexível
CAMINHO_CSV = Path(__file__).resolve().parent.parent / 'data' / 'trusted' / '2024'  / 'empenhos' / 'empenhos_2024_processadas.xlsx'


# --- Funções de API ---
def consultar_pagamentos_relacionados(codigo_empenho):
    """
    Consulta os pagamentos relacionados a um empenho específico.
    Utiliza fase 4 para Pagamentos.
    """
    pagina = 1
    resultados = []
    while True:
        url = 'https://api.portaldatransparencia.gov.br/api-de-dados/despesas/documentos-relacionados'
        params = {
            'codigoDocumento': codigo_empenho,
            'fase': 1,  # Fase 4 para Pagamentos
            'pagina': pagina,
        }
        headers = {
            'chave-api-dados': API_KEY,
            'accept': '*/*'
        }
        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
        except requests.exceptions.RequestException as err:
            print(f"❌ Erro na requisição para o empenho {codigo_empenho}: {err}")
            return None # Retorna None em caso de erro

        dados = response.json()
        if not dados:
            break

        resultados.extend(dados)
        print(f"✅ Pagamento(s) relacionado(s) ao empenho {codigo_empenho}: Página {pagina} com {len(dados)} registro(s)")
        
        # O limite da API é 500 registros por página
        if len(dados) < 500:
            break
        pagina += 1
        time.sleep(0.5)
    return resultados

# --- Funções de Processamento ---
def processar_valor(valor_str):
    """Converte string de valor para float, tratando diferentes formatos."""
    if pd.isna(valor_str) or valor_str == '':
        return 0.0
    if isinstance(valor_str, (int, float)):
        return float(valor_str)
    
    valor_limpo = str(valor_str).strip()
    # Remove tudo que não for dígito, ponto, vírgula ou sinal de menos
    valor_limpo = ''.join(c for c in valor_limpo if c.isdigit() or c in '.,-')
    
    if ',' in valor_limpo and '.' in valor_limpo:
        valor_limpo = valor_limpo.replace('.', '').replace(',', '.')
    elif ',' in valor_limpo:
        valor_limpo = valor_limpo.replace(',', '.')

    try:
        return float(valor_limpo)
    except ValueError:
        print(f"⚠️ Erro ao converter valor: '{valor_str}'")
        return 0.0

# --- Função Principal de Cálculo ---
def calcular_saldos(caminho_empenhos):
    print(f"🔄 Carregando empenhos de: {caminho_empenhos}")
    try:
        # A coluna 'documento' é o código do empenho e deve ser str
        df_empenhos = pd.read_excel(caminho_empenhos, dtype={'documento': str})
        print(f"✅ {len(df_empenhos)} empenhos carregados.")
    except FileNotFoundError:
        print(f"❌ Erro: Arquivo não encontrado em '{caminho_empenhos}'.")
        return None

    colunas_necessarias = ['documento', 'valor', 'observacao']
    if not all(col in df_empenhos.columns for col in colunas_necessarias):
        print("❌ Erro: Colunas necessárias ausentes.")
        print(f"Colunas disponíveis: {df_empenhos.columns.tolist()}")
        return None

    print("\n--- Iniciando coleta de dados de pagamentos para cada empenho ---")
    resultados_consolidados = []
    detalhes_pagamentos_por_empenho = []

    for index, row in df_empenhos.iterrows():
        codigo_empenho = row['documento']
        valor_empenho = processar_valor(row['valor'])
        observacao_empenho = row['observacao'] if pd.notna(row['observacao']) else ''
        
        print(f"\n🔍 Processando empenho {codigo_empenho}...")
        
        pagamentos_relacionados = consultar_pagamentos_relacionados(codigo_empenho)
        
        total_pago = 0.0
        detalhes_pagamentos_do_empenho = []
        if pagamentos_relacionados:
            for pagamento in pagamentos_relacionados:
                valor_pago = processar_valor(pagamento.get('valor', 0))
                total_pago += valor_pago
                detalhes_pagamentos_do_empenho.append({
                    'codigo_empenho_origem': codigo_empenho,
                    'valor_pago': valor_pago,
                    'data_pagamento': pagamento.get('dataPagamento', ''),
                    'detalhes_pagamento': pagamento
                })

        saldo = valor_empenho - total_pago
        
        resultado = {
            'codigo_empenho': codigo_empenho,
            'valor_empenho': valor_empenho,
            'total_pago': total_pago,
            'saldo': saldo,
            'percentual_executado': (total_pago / valor_empenho * 100) if valor_empenho > 0 else 0,
            'observacao': observacao_empenho,
            'qtd_pagamentos': len(pagamentos_relacionados) if pagamentos_relacionados else 0
        }
        
        resultados_consolidados.append(resultado)
        detalhes_pagamentos_por_empenho.extend(detalhes_pagamentos_do_empenho)

        print(f"   💰 Valor Empenhado: R$ {valor_empenho:,.2f}")
        print(f"   💸 Total Pago: R$ {total_pago:,.2f}")
        print(f"   💳 Saldo: R$ {saldo:,.2f}")
        print(f"   📊 Execução: {resultado['percentual_executado']:.1f}%")

    return pd.DataFrame(resultados_consolidados), pd.DataFrame(detalhes_pagamentos_por_empenho)

# --- Execução do Script Principal ---
if __name__ == "__main__":
    df_saldos, df_detalhes_pagamentos = calcular_saldos(CAMINHO_CSV)
    if df_saldos is not None:
        # Cria pasta de saída se não existir
        pasta_saida = Path(__file__).resolve().parent.parent / 'data' / 'refined' / 'analise_empenhos'
        os.makedirs(pasta_saida, exist_ok=True)
        print(f"\n📁 Pasta '{pasta_saida}' verificada.")

        # Salva arquivo principal de saldos
        caminho_saldos = pasta_saida / 'saldos_empenhos_2024.csv'
        df_saldos.to_csv(caminho_saldos, index=False, sep=';', decimal=',')
        print(f"✅ Saldos consolidados salvos em: {caminho_saldos}")

        # Salva detalhes dos pagamentos (se houver)
        if not df_detalhes_pagamentos.empty:
            caminho_pagamentos = pasta_saida / 'detalhes_pagamentos_2024.csv'
            df_detalhes_pagamentos.to_csv(caminho_pagamentos, index=False, sep=';', decimal=',')
            print(f"✅ Detalhes dos pagamentos salvos em: {caminho_pagamentos}")

        # --- Relatório Final ---
        print("\n" + "="*60)
        print("📊 RELATÓRIO FINAL - ANÁLISE DE SALDOS DE EMPENHOS")
        print("="*60)
        if df_saldos is not None:
            total_empenhado = df_saldos['valor_empenho'].sum()
            total_pago = df_saldos['total_pago'].sum()
            saldo_total = df_saldos['saldo'].sum()
            
            # AQUI ESTÁ A MUDANÇA: Novas contagens mais precisas
            empenhos_saldo_positivo = len(df_saldos[df_saldos['saldo'] > 0])
            empenhos_saldo_zero = len(df_saldos[df_saldos['saldo'] == 0])
            empenhos_saldo_negativo = len(df_saldos[df_saldos['saldo'] < 0])
            
            print(f"💰 Total Empenhado: R$ {total_empenhado:,.2f}")
            print(f"💸 Total Pago: R$ {total_pago:,.2f}")
            print(f"💳 Saldo Total: R$ {saldo_total:,.2f}")
            print(f"📊 Execução Geral: {(total_pago/total_empenhado*100):.1f}%")
            print(f"📈 Empenhos com Saldo Positivo: {empenhos_saldo_positivo}")
            print(f"⚖️  Empenhos Totalmente Executados (Saldo Zero): {empenhos_saldo_zero}")
            print(f"📉 Empenhos com Saldo Negativo: {empenhos_saldo_negativo}")
        # Top 10 maiores saldos
        print(f"\n🔝 TOP 10 MAIORES SALDOS:")
        top_saldos = df_saldos.nlargest(10, 'saldo')[['codigo_empenho', 'valor_empenho', 'saldo', 'observacao']]
        for idx, empenho in top_saldos.iterrows():
            print(f"   {empenho['codigo_empenho']}: R$ {empenho['saldo']:,.2f} (de R$ {empenho['valor_empenho']:,.2f})")
        # Top 10 menores saldos
        print(f"\n🔻 TOP 10 MENORES SALDOS:")
        top_menores_saldos = df_saldos.nsmallest(10, 'saldo')[['codigo_empenho', 'valor_empenho', 'saldo', 'observacao']]
        for idx, empenho in top_menores_saldos.iterrows():
            print(f"   {empenho['codigo_empenho']}: R$ {empenho['saldo']:,.2f} (de R$ {empenho['valor_empenho']:,.2f})")

        print("\n🚀 Processamento finalizado com sucesso!")