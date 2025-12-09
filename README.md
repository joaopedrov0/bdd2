# 💳 Detector de Fraude em Cartão de Crédito – BDD2

Aplicação em Python que implementa um pipeline de **engenharia de dados + machine learning** para detectar possíveis fraudes em transações de cartão de crédito.  
O sistema baixa o dataset, particiona os dados, grava em bancos distintos (MySQL e MongoDB) e treina um modelo de classificação.

---

## ⚙️ Tecnologias utilizadas

- **Linguagem:** Python 3
- **Bancos de dados:** MySQL (Docker), MongoDB Atlas ou local, SQLite
- **Bibliotecas:** pandas, numpy, scikit-learn, matplotlib
- **Infra:** Docker, Ubuntu Server (VM da prova)

---

## 💻 Pré-requisitos

- Máquina virtual **Ubuntu Server 25.10 – JCRBDD2-2aProva.ova** ligada e com internet
- Docker instalado e funcionando para o usuário (`docker ps` sem erro)
- Python 3 com suporte a `venv`
- Conta no **Kaggle** (para download do dataset)
- (Opcional) Conta no **MongoDB Atlas**

---

## 📂 Onde rodar

Toda a execução acontece dentro da pasta:

```bash
cd bdd2/machine-learning

```

## 🚀 Passo a passo de execução

 - 1. Clonar o repositório
      
```bash
git clone https://github.com/joaopedrov0/bdd2.git
cd bdd2/machine-learning
```

- 2. Criar e ativar o ambiente virtual
     
```bash
python3 -m venv .venv
source .venv/bin/activate
```

- 3. Instalar as dependências
     
```bash
pip install -r requirements.txt
```

- 4. Baixar e preparar o CSV (Kaggle)
     
```bash
python3 baixarcsv.py
```

- 5. Executar o pipeline completo

```bash
python3 main.py
```

## 👥 Autores
- Fabio Vivarelli
- João Pedro Veríssimo Goncalves
- João Vitor Gimenes dos Santos
- Juan Santos Trigo Nasser
- Nathan Henrique Guimaraes de Oliveira

🔗 link documento:
https://docs.google.com/document/d/1rH32h3iwNiaM5Igdz7zjDOJG6fYtb-NlGOQyRZ_bFNo/edit?usp=sharing
