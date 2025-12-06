import kagglehub
import pandas as pd
from sqlalchemy import create_engine
import os

path = kagglehub.dataset_download("mlg-ulb/creditcardfraud")
arquivo_csv = os.path.join(path, "creditcard.csv")

df = pd.read_csv(arquivo_csv)

total_linhas = len(df)
metade = total_linhas // 2

df_parte1 = df.iloc[:metade]
df_parte2 = df.iloc[metade:]

print(f"Total de linhas: {total_linhas}")
print(f"Parte 1 (MySQL): {len(df_parte1)} linhas")
print(f"Parte 2 (MongoDB): {len(df_parte2)} linhas")

print("CONECTANDO NO BANCO, SE FALHAR É PQ A SENHA NÃO É 'root' ARQUIVO feed_db.py NA LINHA 23 (troca o segundo 'root' pela sua senha diferentona)")

db_connection_str = 'mysql+mysqlconnector://root:root@localhost:3307/Creditcard'
db_connection = create_engine(db_connection_str)

# sim esse final foi vibe-coded

try:
    print("--- Inserindo dados no MySQL (Isso pode demorar um pouco)... ---")
    # 'to_sql' cria a tabela automaticamente se ela não existir
    df_parte1.to_sql(name='TransacoesBronze', con=db_connection, if_exists='replace', index=False, chunksize=1000)
    print("Sucesso! Dados inseridos no MySQL.")
except Exception as e:
    print(f"Erro ao conectar ou inserir no MySQL: {e}")
    
json_path = "credit-card2.json"
print(f"--- Salvando Parte 2 como JSON em {json_path}... ---")
df_parte2.to_json(json_path, orient='records', lines=True)
print("Concluído.")