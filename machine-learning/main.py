#!/usr/bin/env python3
import os
import sys
import time
import json
import logging
import subprocess
import tempfile
from pathlib import Path
from typing import Tuple, Optional

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("credit_etl")

# ---------- Helper: ensure packages ----------
def ensure_packages():
    """
    Try to import required packages; if missing, pip install them.
    """
    # packages to try to ensure (pip names)
    needed = [
        "pandas",
        "numpy",
        "mysql-connector-python",
        "pymongo",
        "SQLAlchemy",
        "scikit-learn",
        "matplotlib"
    ]
    missing = []
    # map pip package -> import name guess (simple heuristic)
    for pkg in needed:
        import_name = pkg.split("-")[0]
        # special-case scikit-learn import name
        if pkg == "scikit-learn":
            import_name = "sklearn"
        if pkg == "mysql-connector-python":
            import_name = "mysql"
        if pkg == "SQLAlchemy":
            import_name = "sqlalchemy"
        try:
            __import__(import_name)
        except Exception:
            missing.append(pkg)
    if missing:
        log.info("Pacotes faltando: %s. Tentando instalar via pip.", ", ".join(missing))
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", *missing])
            log.info("Instalação de pacotes concluída.")
        except Exception as e:
            log.error("Falha ao instalar dependencias via pip: %s", e)
            log.error("Tente instalar manualmente: pip install %s", " ".join(missing))
            # não interromper; tentaremos prosseguir e falhar mais tarde se necessário.

ensure_packages()

import pandas as pd
import numpy as np
import sqlite3
from sqlalchemy import create_engine, text
import mysql.connector
from pymongo import MongoClient, errors as pymongo_errors

# ---------- Configuráveis ----------
BASE_DIR = Path.cwd()
DATA_DIR = BASE_DIR / "data"
BRONZE_DIR = BASE_DIR / "lakehouse" / "bronze"
SILVER_DIR = BASE_DIR / "lakehouse" / "silver"
GOLD_DIR = BASE_DIR / "lakehouse" / "gold"
MONGO_LOCAL_DIR = BASE_DIR / "mongo_local"
SQLITE_PATH = GOLD_DIR / "gold_db.sqlite3"
CREDIT_CSV = BASE_DIR / "credit-card.csv"
CREDIT_CSV_1 = BASE_DIR / "credit-card1.csv"
CREDIT_CSV_2 = BASE_DIR / "credit-card2.csv"
CREDIT_JSON_2 = BASE_DIR / "credit-card2.json"

MYSQL_CONTAINER_NAME = "credit_etl_mysql_tmp"
MYSQL_ROOT_PASSWORD = "TmpPassw0rd!"
MYSQL_DB = "credit_etl_db"
MYSQL_USER = "root"
MYSQL_PORT = 3307  # escolha um porto alternativo para evitar conflitos

MONGO_CONTAINER_NAME = "credit_etl_mongo_tmp"
MONGO_PORT = 27018  # porto alternativo

AIRFLOW_DAG_FILENAME = BASE_DIR / "airflow_dag_credit_etl.py"

# ---------- Utilitários Docker ----------
def docker_available() -> bool:
    try:
        subprocess.check_call(["docker", "ps"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        log.debug("Docker disponível.")
        return True
    except Exception:
        log.warning("Docker não disponível no sistema (ou usuário não tem permissão).")
        return False

def run_subprocess(cmd, check=True):
    log.debug("Executando: %s", " ".join(str(x) for x in cmd))
    return subprocess.run(cmd, check=check, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

# ---------- Data generation if csv missing ----------
def generate_example_dataset(path: Path, n=1000, random_state=42):
    """
    Gera um dataset sintético semelhante a datasets de fraude de cartão de crédito
    Columns: time, amount, V1..V10 (features), class (0/1)
    """
    log.info("Gerando dataset sintético em %s (n=%d).", path, n)
    rng = np.random.RandomState(random_state)
    # Simples: 10 features + amount + time + class (imbalanced)
    X = rng.normal(size=(n, 10))
    amount = np.abs(rng.normal(loc=50, scale=120, size=(n, 1))).flatten()
    time_col = np.arange(n)
    # Make class imbalanced: ~2% fraud
    classes = (rng.rand(n) < 0.02).astype(int)
    cols = {f"V{i+1}": X[:, i] for i in range(10)}
    df = pd.DataFrame(cols)
    df["amount"] = amount
    df["time"] = time_col
    df["class"] = classes
    df.to_csv(path, index=False)
    log.info("Dataset sintético salvo em %s", path)
    return df

# ---------- Split CSV ----------
def split_csv(full_path: Path) -> Tuple[Path, Path]:
    if not full_path.exists():
        log.warning("%s não encontrado. Irei gerar um dataset de exemplo.", full_path)
        generate_example_dataset(full_path, n=2000)
    log.info("Lendo CSV completo de %s", full_path)
    df = pd.read_csv(full_path)
    mid = len(df) // 2
    df1 = df.iloc[:mid].reset_index(drop=True)
    df2 = df.iloc[mid:].reset_index(drop=True)
    df1.to_csv(CREDIT_CSV_1, index=False)
    df2.to_csv(CREDIT_CSV_2, index=False)
    log.info("Dividido em %s (n=%d) e %s (n=%d).", CREDIT_CSV_1, len(df1), CREDIT_CSV_2, len(df2))
    return CREDIT_CSV_1, CREDIT_CSV_2

# ---------- MySQL setup and ingest ----------
def mysql_container_running() -> bool:
    if not docker_available():
        return False
    try:
        out = run_subprocess(["docker", "ps", "--filter", f"name={MYSQL_CONTAINER_NAME}", "--format", "{{.Names}}"], check=True)
        return MYSQL_CONTAINER_NAME in out.stdout
    except Exception:
        return False

def start_mysql_container():
    if not docker_available():
        log.warning("Docker não disponível: não será criado container MySQL. Usaremos fallback SQLite para a camada Gold/MySQL step.")
        return False
    if mysql_container_running():
        log.info("Container MySQL já está executando: %s", MYSQL_CONTAINER_NAME)
        return True
    log.info("Criando container MySQL temporário '%s' na porta %d ...", MYSQL_CONTAINER_NAME, MYSQL_PORT)
    try:
        run_subprocess([
            "docker", "run", "-d",
            "--name", MYSQL_CONTAINER_NAME,
            "-e", f"MYSQL_ROOT_PASSWORD={MYSQL_ROOT_PASSWORD}",
            "-e", f"MYSQL_DATABASE={MYSQL_DB}",
            "-p", f"{MYSQL_PORT}:3306",
            "mysql:8.0",
        ], check=True)
        log.info("Container MySQL iniciado. Aguardando inicialização (pode levar ~20s)...")
        # aguardar o MySQL aceitar conexões
        time.sleep(20)
        return True
    except Exception as e:
        log.error("Falha ao criar container MySQL: %s", e)
        return False

def connect_mysql(retries=8, wait=5):
    for attempt in range(1, retries+1):
        try:
            conn = mysql.connector.connect(
                host="127.0.0.1", port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_ROOT_PASSWORD, database=MYSQL_DB
            )
            log.info("Conectado ao MySQL (porta %d).", MYSQL_PORT)
            return conn
        except Exception as e:
            log.warning("Tentativa %d: não foi possível conectar ao MySQL: %s", attempt, e)
            time.sleep(wait)
    log.error("Não foi possível conectar ao MySQL após %d tentativas.", retries)
    return None

def ingest_csv_to_mysql(csv_path: Path, table_name="credit_cards"):
    if not start_mysql_container():
        log.warning("MySQL não disponível. Pulando etapa de ingest no MySQL e usando fallback SQLite.")
        return False
    conn = connect_mysql()
    if conn is None:
        log.warning("Conexão MySQL falhou. Usando fallback local (SQLite).")
        return False
    df = pd.read_csv(csv_path)
    # infer schema: we'll create table dynamically
    columns = []
    for col in df.columns:
        if pd.api.types.is_integer_dtype(df[col]) or pd.api.types.is_bool_dtype(df[col]):
            sqlt = "BIGINT"
        elif pd.api.types.is_float_dtype(df[col]):
            sqlt = "DOUBLE"
        else:
            sqlt = "TEXT"
        columns.append(f"`{col}` {sqlt}")
    create_sql = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(columns)});"
    log.info("Criando tabela MySQL (se não existir): %s", create_sql)
    cur = conn.cursor()
    cur.execute(create_sql)
    conn.commit()
    # insert rows in batches
    placeholders = ", ".join(["%s"] * len(df.columns))
    colnames = ", ".join([f"`{c}`" for c in df.columns])
    insert_sql = f"INSERT INTO `{table_name}` ({colnames}) VALUES ({placeholders})"
    log.info("Inserindo %d linhas em MySQL na tabela %s", len(df), table_name)
    batch = 500
    rows = df.where(pd.notnull(df), None).values.tolist()
    for i in range(0, len(rows), batch):
        batch_rows = rows[i:i+batch]
        cur.executemany(insert_sql, batch_rows)
        conn.commit()
        log.info("Inseridas linhas %d-%d", i+1, min(i+batch, len(rows)))
    cur.close()
    conn.close()
    log.info("Ingestão para MySQL concluída.")
    return True

# ---------- Mongo setup and ingest ----------
def get_mongo_client_from_env() -> Optional[MongoClient]:
    uri = os.environ.get("MONGODB_URI")
    if not uri:
        return None
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        log.info("Conectado ao MongoDB Atlas via MONGODB_URI.")
        return client
    except Exception as e:
        log.warning("Falha ao conectar ao MONGODB_URI informado: %s", e)
        return None

def start_mongo_container():
    if not docker_available():
        log.warning("Docker não disponível: não será criado container MongoDB.")
        return False
    # check if container exists
    try:
        out = run_subprocess(["docker", "ps", "--filter", f"name={MONGO_CONTAINER_NAME}", "--format", "{{.Names}}"], check=True)
        if MONGO_CONTAINER_NAME in out.stdout:
            log.info("Container MongoDB já em execução: %s", MONGO_CONTAINER_NAME)
            return True
    except Exception:
        pass
    log.info("Criando container MongoDB temporário '%s' na porta %d ...", MONGO_CONTAINER_NAME, MONGO_PORT)
    try:
        run_subprocess([
            "docker", "run", "-d",
            "--name", MONGO_CONTAINER_NAME,
            "-p", f"{MONGO_PORT}:27017",
            "mongo:6.0"
        ], check=True)
        log.info("Container MongoDB iniciado. Aguardando inicialização (~5s).")
        time.sleep(6)
        return True
    except Exception as e:
        log.error("Falha ao criar container MongoDB: %s", e)
        return False

def ingest_json_to_mongo(json_path: Path, db_name="credit_etl", collection_name="credit_cards"):
    client = get_mongo_client_from_env()
    used_local = False
    if client is None:
        if not start_mongo_container():
            log.warning("Mongo Atlas não fornecido e container não criado. Usarei fallback local (salvar JSON em pasta).")
            used_local = True
        else:
            try:
                client = MongoClient("mongodb://127.0.0.1:%d" % MONGO_PORT, serverSelectionTimeoutMS=10000)
                client.admin.command("ping")
                log.info("Conectado ao MongoDB local (docker).")
            except Exception as e:
                log.error("Não consegui conectar ao Mongo local: %s", e)
                used_local = True
    if used_local:
        # fallback: salvar JSON em pasta
        MONGO_LOCAL_DIR.mkdir(parents=True, exist_ok=True)
        target = MONGO_LOCAL_DIR / json_path.name
        with open(json_path, "rb") as fin, open(target, "wb") as fout:
            fout.write(fin.read())
        log.info("Fallback: JSON salvo localmente em %s (simulando inserção no Mongo).", target)
        return False
    # inserir documentos
    with open(json_path, "r", encoding="utf-8") as f:
        docs = json.load(f)
    if not isinstance(docs, list):
        log.warning("Arquivo JSON não contém uma lista de documentos. Convertendo para lista.")
        docs = [docs]
    db = client[db_name]
    coll = db[collection_name]
    try:
        res = coll.insert_many(docs)
        log.info("Inseridos %d documentos no MongoDB no banco %s, coleção %s.", len(res.inserted_ids), db_name, collection_name)
        return True
    except Exception as e:
        log.error("Falha ao inserir documentos no MongoDB: %s", e)
        return False

# ---------- Lakehouse: Bronze/Silver/Gold ----------
def ensure_dirs():
    for d in [BRONZE_DIR, SILVER_DIR, GOLD_DIR]:
        d.mkdir(parents=True, exist_ok=True)
    MONGO_LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

def create_bronze_txt(csv1: Path, csv2: Path):
    ensure_dirs()
    # Bronze = txt files. We'll save the CSV content as .txt as requested.
    txt1 = BRONZE_DIR / "credit-card1.txt"
    txt2 = BRONZE_DIR / "credit-card2.txt"
    txt1.write_text(csv1.read_text())
    txt2.write_text(csv2.read_text())
    log.info("Bronze: arquivos txt criados em %s e %s", txt1, txt2)
    return txt1, txt2

def create_silver_dataframe(csv1: Path, csv2: Path):
    # Silver: features relevantes + agregações (dataframe)
    # Carregar ambos e concatenar; selecionar colunas numéricas e class
    df1 = pd.read_csv(csv1)
    df2 = pd.read_csv(csv2)
    df = pd.concat([df1, df2], ignore_index=True)
    # selecionar colunas numéricas
    num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    if "class" in df.columns:
        if "class" not in num_cols:
            num_cols.append("class")
    silver_df = df[num_cols].copy()
    # agregações: média e std por 'class' se existir
    if "class" in silver_df.columns:
        agg = silver_df.groupby("class").agg(["mean", "std"])
        # transformar para dataframe "flattened"
        agg.columns = ["_".join(col).strip() for col in agg.columns.values]
        # juntamos a agregação como um dataframe separado
        silver_agg = agg.reset_index()
        silver_pickle = SILVER_DIR / "silver_dataframe.pkl"
        silver_agg.to_pickle(silver_pickle)
        log.info("Silver: dataframe agregado salvo em %s", silver_pickle)
        return silver_agg
    else:
        silver_pickle = SILVER_DIR / "silver_dataframe.pkl"
        silver_df.to_pickle(silver_pickle)
        log.info("Silver: dataframe salvo em %s", silver_pickle)
        return silver_df

def create_gold_sqlite(csv1: Path, csv2: Path):
    # Gold: tabelas relacionais usando SQLite
    ensure_dirs()
    df1 = pd.read_csv(csv1)
    df2 = pd.read_csv(csv2)
    engine = create_engine(f"sqlite:///{SQLITE_PATH}")
    # salvar duas tabelas: credit_cards_source_mysql (from first half) and credit_cards_mongo (from second half)
    log.info("Gold: salvando tabelas no SQLite em %s", SQLITE_PATH)
    df1.to_sql("credit_cards_source_mysql", con=engine, if_exists="replace", index=False)
    df2.to_sql("credit_cards_source_mongo", con=engine, if_exists="replace", index=False)
    log.info("Gold: tabelas 'credit_cards_source_mysql' e 'credit_cards_source_mongo' criadas no SQLite.")
    return SQLITE_PATH

# ---------- Convert second half to JSON ----------
def csv_to_json(csv_path: Path, json_path: Path):
    df = pd.read_csv(csv_path)
    records = df.where(pd.notnull(df), None).to_dict(orient="records")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(records, f, default=str, ensure_ascii=False, indent=2)
    log.info("Arquivo JSON criado em %s a partir de %s (n=%d docs).", json_path, csv_path, len(records))
    return json_path

# ---------- Airflow DAG generator ----------
def generate_airflow_dag(script_path: Path, dag_path: Path = AIRFLOW_DAG_FILENAME):
    """
    Gera um DAG simples que executa este script via BashOperator.
    Instruções:
      - Copie o arquivo gerado para $AIRFLOW_HOME/dags/
      - Reinicie o scheduler se necessário.
    """
    dag_content = f"""\
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {{
    'owner': 'student',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}}

with DAG(
    dag_id='credit_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for credit card fraud (student project)',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    run_etl = BashOperator(
        task_id='run_main_etl',
        bash_command='python3 {script_path} 2>&1 | tee {BASE_DIR / "etl_run.log"}'
    )
"""
    dag_path.write_text(dag_content)
    log.info("DAG do Airflow gerado em %s. Copie para AIRFLOW_HOME/dags e reinicie Airflow se necessário.", dag_path)
    return dag_path

# ---------- Modeling & Visualization (ADDED at the end as requested) ----------
def run_model_and_visualize(csv_path: Path, output_dir: Path):
    """
    Treina um classificador simples usando divisão 70/20/10 (train/val/test),
    mostra matriz de confusão no conjunto de teste e imprime alguns exemplos
    de verdadeiros/falsos positivos/negativos.
    """
    # local import to avoid dependency issues earlier
    try:
        from sklearn.model_selection import train_test_split
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.metrics import confusion_matrix, classification_report
        import matplotlib.pyplot as plt
    except Exception as e:
        log.error("Dependências de ML ausentes ou falha ao importar: %s", e)
        log.info("Tentando garantir pacotes adicionais e reexecutar importações.")
        ensure_packages()
        from sklearn.model_selection import train_test_split
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.metrics import confusion_matrix, classification_report
        import matplotlib.pyplot as plt

    output_dir.mkdir(parents=True, exist_ok=True)
    log.info("Carregando dados para modelagem a partir de %s", csv_path)
    df = pd.read_csv(csv_path)

    if "class" not in df.columns:
        log.error("Coluna 'class' não encontrada no dataset; cancelando etapa de modelagem.")
        return

    # features: todas colunas numéricas exceto 'class' e 'time' (se existir)
    feature_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    # remove target and time from features if present
    if "class" in feature_cols:
        feature_cols.remove("class")
    if "time" in feature_cols:
        feature_cols.remove("time")

    X = df[feature_cols].fillna(0).values
    y = df["class"].values

    # Step 1: train (70%) and temp (30%)
    X_train, X_temp, y_train, y_temp, idx_train, idx_temp = train_test_split(
        X, y, df.index.values, test_size=0.30, random_state=42, stratify=y
    )
    # Step 2: split temp into validation (20% total -> 2/3 of temp) and test (10% total -> 1/3 of temp)
    # within temp, test_size = 1/3 gives test = 0.30 * (1/3) = 0.10 total
    X_val, X_test, y_val, y_test, idx_val, idx_test = train_test_split(
        X_temp, y_temp, idx_temp, test_size=1/3, random_state=42, stratify=y_temp
    )

    log.info("Divisão de dados: treino=%d (%.1f%%), validação=%d (%.1f%%), teste=%d (%.1f%%)",
             len(X_train), len(X_train)/len(X)*100,
             len(X_val), len(X_val)/len(X)*100,
             len(X_test), len(X_test)/len(X)*100)

    # Treinar classificador simples
    clf = RandomForestClassifier(n_estimators=100, random_state=42, class_weight="balanced")
    clf.fit(X_train, y_train)
    log.info("Modelo treinado (RandomForestClassifier).")

    # Previsões no conjunto de teste
    y_pred = clf.predict(X_test)

    # Matriz de confusão e relatório
    cm = confusion_matrix(y_test, y_pred)
    report = classification_report(y_test, y_pred, digits=4)
    log.info("Confusion matrix (test):\n%s", cm)
    log.info("Classification report (test):\n%s", report)

    # Plot da matriz de confusão
    try:
        fig, ax = plt.subplots(figsize=(5, 4))
        im = ax.imshow(cm, interpolation='nearest')
        ax.set_title("Matriz de Confusão (Test)")
        ax.set_xlabel("Predito")
        ax.set_ylabel("Real")
        ax.set_xticks([0, 1])
        ax.set_yticks([0, 1])
        ax.set_xticklabels(["Não-Fraude (0)", "Fraude (1)"])
        ax.set_yticklabels(["Não-Fraude (0)", "Fraude (1)"])
        # escrever os valores dentro das células
        thresh = cm.max() / 2.
        for i in range(cm.shape[0]):
            for j in range(cm.shape[1]):
                ax.text(j, i, format(cm[i, j], 'd'),
                        ha="center", va="center",
                        color="white" if cm[i, j] > thresh else "black")
        fig.tight_layout()
        cm_path = output_dir / "confusion_matrix_test.png"
        fig.savefig(str(cm_path))
        plt.close(fig)
        log.info("Matriz de confusão salva em %s", cm_path)
    except Exception as e:
        log.warning("Falha ao gerar/Salvar figura da matriz de confusão: %s", e)

    # Mostrar alguns exemplos detectados FRAUDE e NÃO FRAUDE
    # Reconstruir DataFrame de teste original para extrair linhas
    df_test = df.loc[idx_test].copy()
    df_test = df_test.reset_index(drop=True)
    # garantir que as previsões estejam alinhadas
    df_test["_pred"] = y_pred
    df_test["_real"] = y_test

    def sample_and_log(cond, label, max_samples=5):
        subset = df_test.loc[cond]
        n = len(subset)
        if n == 0:
            log.info("Nenhum caso encontrado para %s.", label)
            return subset
        sampled = subset.sample(n=min(max_samples, n), random_state=42)
        log.info("Exemplos (%d amostrados de %d) para %s:", len(sampled), n, label)
        # print columns concisely
        with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 200):
            log.info("\n%s", sampled.head(50).to_string(index=False))
        return sampled

    tp = sample_and_log((df_test["_pred"] == 1) & (df_test["_real"] == 1), "True Positives (Predito=1 & Real=1)")
    fp = sample_and_log((df_test["_pred"] == 1) & (df_test["_real"] == 0), "False Positives (Predito=1 & Real=0)")
    fn = sample_and_log((df_test["_pred"] == 0) & (df_test["_real"] == 1), "False Negatives (Predito=0 & Real=1)")
    tn = sample_and_log((df_test["_pred"] == 0) & (df_test["_real"] == 0), "True Negatives (Predito=0 & Real=0)")

    # salvar exemplos em CSV para inspeção
    try:
        samples_out = output_dir / "prediction_examples_test_samples.csv"
        pd.concat([tp, fp, fn, tn], ignore_index=True).to_csv(samples_out, index=False)
        log.info("Exemplos amostrados salvos em %s", samples_out)
    except Exception as e:
        log.warning("Falha ao salvar exemplos amostrados: %s", e)

    # também salvar o relatório de classificação
    try:
        (output_dir / "classification_report_test.txt").write_text(report)
        log.info("Relatório de classificação salvo em %s", output_dir / "classification_report_test.txt")
    except Exception as e:
        log.warning("Falha ao salvar relatório: %s", e)


# ---------- Main flow ----------
def main():
    log.info("INICIANDO PIPELINE ETL - Plataforma Detecção de Fraudes (início)")

    ensure_dirs()

    # 1) Split CSV or generate if missing
    csv1, csv2 = split_csv(CREDIT_CSV)

    # 2) Bronze
    create_bronze_txt(csv1, csv2)

    # 3) Ingest half1 -> MySQL (or fallback)
    mysql_ok = ingest_csv_to_mysql(csv1, table_name="credit_cards")

    if not mysql_ok:
        log.info("Executando fallback: salvando primeira metade também no SQLite (Gold fallback).")
        create_gold_sqlite(csv1, csv2)  # this will create gold tables including both halves

    # 4) Convert half2 to JSON and ingest into Mongo
    json2 = csv_to_json(csv2, CREDIT_JSON_2)
    mongo_ok = ingest_json_to_mongo(json2)

    if not mongo_ok:
        log.info("MongoDB não disponível; documento JSON salvo localmente em %s", MONGO_LOCAL_DIR)

    # 5) Silver transformation
    silver = create_silver_dataframe(csv1, csv2)

    # 6) Gold (relational) storage
    sqlite_path = create_gold_sqlite(csv1, csv2)

    # 7) Generate Airflow DAG file to call this script
    try:
        generate_airflow_dag(Path(__file__).absolute())
    except Exception:
        # in some execution contexts __file__ may not be defined (e.g., interactive), skip gracefully
        log.debug("Não foi possível gerar DAG (provavelmente __file__ não está definido neste contexto).")

    # 8) Summary logs
    log.info("PIPELINE CONCLUÍDO.")
    log.info("Resumo dos artefatos gerados:")
    log.info(" - Bronze: %s (txt files under %s)", BRONZE_DIR, BRONZE_DIR)
    log.info(" - Silver: %s (pickle under %s)", SILVER_DIR, SILVER_DIR)
    log.info(" - Gold: SQLite DB at %s", sqlite_path)
    if mysql_ok:
        log.info(" - MySQL: dados inseridos no container (%s) na base %s (porta %d).", MYSQL_CONTAINER_NAME, MYSQL_DB, MYSQL_PORT)
    else:
        log.info(" - MySQL: NÃO disponível. Fallback usado (SQLite).")
    if mongo_ok:
        log.info(" - MongoDB: documentos inseridos (remote/local as configured).")
    else:
        log.info(" - MongoDB: NÃO disponível. JSON salvo localmente em %s", MONGO_LOCAL_DIR)

    log.info("Logs detalhados em stdout e em %s/etl_run.log quando executado via DAG.", BASE_DIR)

if __name__ == "__main__":
    main()
    # Ao final, executar visualização de resultados conforme pedido:
    # Usamos a tabela gold (segunda metade) para treinar/testar o modelo e gerar exemplos.
    # Preferimos usar o CSV completo se existir; cair em CSV de exemplo se não.
    try:
        # preferir o CSV original completo; se não existir usamos a segunda metade (csv2)
        model_csv = CREDIT_CSV if CREDIT_CSV.exists() else CREDIT_CSV_2
        if not model_csv.exists():
            log.warning("Nenhum CSV disponível para modelagem (%s e %s ausentes). Gerando dataset sintético temporário.", CREDIT_CSV, CREDIT_CSV_2)
            generate_example_dataset(CREDIT_CSV, n=2000)
            model_csv = CREDIT_CSV
        out_dir = GOLD_DIR / "model_results"
        run_model_and_visualize(model_csv, out_dir)
    except Exception as e:
        log.error("Erro na etapa de modelagem/visualização: %s", e)
