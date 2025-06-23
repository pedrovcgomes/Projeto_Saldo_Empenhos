import requests
import pandas as pd
import os

# Configurações
CNPJ_EMPRESA = "03045711000170"
ANO = 2025
ORDENACAO = 4  # Data decrescente
API_KEY = "***REMOVED_API_KEY_PLACEHOLDER***"
FASENOMES = {
    1: "Empenhos",
    2: "Liquidações",
    3: "Pagamentos"
}

# Garante que a pasta data exista
os.makedirs("data", exist_ok=True)

# Função para coletar os dados de uma fase
def coletar_dados_por_fase(fase):
    pagina = 1
    resultados = []

    while True:
        url = "https://api.portaldatransparencia.gov.br/api-de-dados/despesas/documentos-por-favorecido"
        params = {
            "codigoPessoa": CNPJ_EMPRESA,
            "fase": fase,
            "ano": ANO,
            "pagina": pagina,
            "ordenacaoResultado": ORDENACAO
        }
        headers = {
            "chave-api-dados": API_KEY,
            "accept": "*/*"
        }

        response = requests.get(url, params=params, headers=headers)

        if response.status_code != 200:
            print(f"⚠️ Erro na fase {fase}, página {pagina}: status {response.status_code}")
            break

        dados = response.json()
        if not dados:
            break  # Fim dos dados

        resultados.extend(dados)
        print(f"✅ {FASENOMES[fase]}: Página {pagina} com {len(dados)} registros")
        pagina += 1

    return pd.DataFrame(resultados)

# Loop pelas fases e coleta dos dados
with pd.ExcelWriter("data/despesas_2025.xlsx", engine="xlsxwriter") as writer:
    for fase in [1, 2, 3]:
        df = coletar_dados_por_fase(fase)
        df.to_excel(writer, sheet_name=FASENOMES[fase], index=False)

print("📁 Arquivo salvo: data/despesas_2025.xlsx")
