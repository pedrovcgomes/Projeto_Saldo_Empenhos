import json
import pandas as pd
import logging
from datetime import datetime
from decimal import Decimal
from api_client import (
    consultar_dados_por_fase,
    consultar_empenho_impactado,
    consultar_documentos_relacionados,
    coletar_itens_empenho_completos,
    formatar_valor,
    FASENOMES
)


# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        # Use o parâmetro 'encoding' para o FileHandler
        logging.FileHandler('balance_calculation.log', encoding='utf-8'),
        # E para o StreamHandler (saída no console)
        logging.StreamHandler(stream=open(1, 'w', encoding='utf-8', errors='ignore'))
    ]
)
logger = logging.getLogger(__name__)

def calcular_saldo_empenho(codigo_empenho: str) -> dict:
    """
    Calcula o saldo atualizado de UM empenho:
    - Soma inclusões e reforços
    - Subtrai anulações
    - Subtrai pagamentos líquidos (consultando API)
    Retorna dicionário com totais.
    """
    logging.info(f"🔎 Analisando empenho {codigo_empenho}")

    # 1. Coleta histórico completo
    historico = coletar_itens_empenho_completos(codigo_empenho)
    valor_inicial = Decimal("0")
    valor_reforco = Decimal("0")
    valor_anulado = Decimal("0")

    for seq, eventos in historico.items():
        for evento in eventos:
            valor_str = evento.get("valorOperacao", "0,00")
            valor = Decimal(valor_str.replace(".", "").replace(",", "."))
            tipo = evento.get("tipoOperacao")

            if tipo == "INCLUSAO":
                valor_inicial += valor
            elif tipo == "REFORCO":
                valor_reforco += valor
            elif tipo == "ANULACAO":
                valor_anulado += valor

    valor_atualizado = valor_inicial + valor_reforco - valor_anulado
    logging.info(f"📊 Valor atualizado do empenho: R$ {formatar_valor(valor_atualizado)}")

    # 2. Consulta documentos relacionados
    docs = consultar_documentos_relacionados(codigo_empenho)

    total_liquidado = Decimal("0")
    total_pago = Decimal("0")

    for doc in docs:
        codigo_doc = doc.get("codigoDocumento")
        fase = doc.get("fase")
        if fase == 2:  # liquidação
            total_liquidado += consultar_empenho_impactado(codigo_doc, 2, codigo_empenho)
        elif fase == 3:  # pagamento
            total_pago += consultar_empenho_impactado(codigo_doc, 3, codigo_empenho)

    saldo = valor_atualizado - total_pago

    return {
        "codigo_empenho": codigo_empenho,
        "valor_inicial": valor_inicial,
        "valor_reforco": valor_reforco,
        "valor_anulado": valor_anulado,
        "valor_atualizado": valor_atualizado,
        "total_liquidado": total_liquidado,
        "total_pago": total_pago,
        "saldo": saldo
    }


def calcular_saldos_empenhos(ano: int = 2024):
    """
    Calcula saldos de TODOS os empenhos do ano informado.
    Salva resultado em CSV.
    """
    logging.info(f"📥 Coletando empenhos do ano {ano}...")
    df_empenhos = consultar_dados_por_fase(1, ano)
    # verificando  a estrutura do df_empenhos
    df_empenhos.head()
    
    resultados = []
    for codigo_empenho in df_empenhos["documento"]:
        try:
            resultado = calcular_saldo_empenho(codigo_empenho)
            resultados.append(resultado)
        except Exception as e:
            logging.error(f"Erro ao processar {codigo_empenho}: {e}")

    df_resultado = pd.DataFrame(resultados)
    df_resultado.to_csv(f"data/refined/saldos_empenhos_{ano}.csv", index=False)
    logging.info(f"✅ Resultado salvo em data/refined/saldos_empenhos_{ano}.csv")


if __name__ == "__main__":
    calcular_saldos_empenhos(2024)