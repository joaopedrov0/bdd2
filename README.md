# 💳 Detector de Fraude em Cartões de Crédito

Este projeto implementa um pipeline completo de **download, preparação, treinamento e avaliação** de um modelo de Machine Learning capaz de detectar transações potencialmente fraudulentas em cartões de crédito.

A aplicação foi projetada para ser **simples, direta e reproduzível**, podendo ser executada em qualquer máquina com Python e Docker configurados.

---

## ⚙️ Tecnologias utilizadas

* **Python 3**
* **Docker** (para ambientes isolados ao rodar pip dentro de containers quando necessário)
* **Machine Learning:** scikit-learn
* **Manipulação de dados:** pandas, numpy
* **Visualização:** matplotlib, seaborn
* **Logs detalhados** em todas as etapas

---

## 💻 Pré-requisitos

Antes de rodar o projeto em uma nova máquina, siga os passos abaixo.

### ✔️ 1. Verificar se o Docker funciona para o seu usuário

No terminal, execute:

```
docker ps
```

Se **não houver erros**, está tudo pronto.
Caso apareça erro de permissão, execute:

```
sudo groupadd docker
sudo usermod -aG docker $USER
newgrp docker
docker run hello-world
reboot
```

---

### ✔️ 2. Criar e ativar o ambiente virtual Python

Dentro da pasta do projeto:

```
python3 -m venv .venv
source .venv/bin/activate
```

### ✔️ 3. Baixar as dependências

```
pip install -r requirements.txt
```

### ✔️ 4. Levantar o container docker com o MySQL e alimentar ele com a primeira metade dos dados

```
docker-compose up -d
python feed_db.py
```

---

### ✔️ 5. Inicializar o algoritmo de Machine Learning

Execute o script responsável por baixar e preparar o CSV:

```
deactivate
cd machine-learning
python3 -m venv .venv
source .env/bin/activate
pip install -r requirements.txt
python baixarcsv.py
```

Esse passo irá:

* Baixar o dataset do Kaggle
* Renomear colunas quando necessário
* Salvar **credit-card.csv** na pasta atual

---

### ✔️ 6. Rodar o pipeline completo

Execute:

```
python main.py
```

O script fará:

* Leitura do arquivo `credit-card.csv`
* Divisão dos dados (70% treino, 20% validação, 10% teste)
* Treinamento do modelo
* Exibição da **matriz de confusão**
* Visualização de exemplos reais e fraudulentos previstos
* Geração de logs detalhados para cada etapa

---

## 📊 Sobre o modelo

O projeto utiliza um classificador voltado a problemas de **altamente desbalanceados**, com técnicas de normalização e métricas adequadas, exibindo:

* Acurácia
* Precisão
* Recall
* F1-score
* Matriz de confusão
* Exemplos onde o modelo acertou e errou fraudes

---

## 📦 Estrutura do projeto (exemplo sugerido)

```
/
├── baixarcsv.py
├── main.py
├── credit-card.csv
├── README.md
└── .venv/
```

---

## 🧪 Resultados

O `main.py` mostra na tela:

* Gráficos da matriz de confusão
* Percentual de detecção de fraudes
* Exemplos reais comentados:

  * Casos detectados como **FRAUDE**
  * Casos detectados como **NÃO FRAUDE**

---

## 👥 Autores

* Projeto acadêmico baseado no dataset "Credit Card Fraud Detection"
* Implementação e adaptação: 


---
