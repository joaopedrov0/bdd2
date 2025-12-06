# Projeto de banco de dados

objetivo aq é fazer um pipeline q vai fazer umas parada

- cria um mysql dentro de um container docker
- cria uma conexão mongodb via nuvem (mdb atlas)

agora vem a parte divertida

- pega o arquivo `credit-card.csv`
- quebra ele no meio
- agora voce tem `credit-card.csv` (original, deixa salvo ai por precauçao), `credit-card1.csv` (primeira metade), `credit-card2.csv` (segunda metade)
- agora tu vai pegar o `credit-card1.csv` e fazer o banco mysql consumir ele
- depois tu pega o `credit-card2.csv`, transforma ele em um json e manda pro mongodb

## arquitetura MEDALHÃO (*Medallion Architecture*)

![imaage](https://tse2.mm.bing.net/th/id/OIP.EdxM4AhRbjS_Gs12Tr_RwAHaDk?cb=ucfimg2&ucfimg=1&rs=1&pid=ImgDetMain&o=7&rm=3)

### paradas brass (bronze)

- guardar dados como texto (como?)

### paradas silver (prata)

![image](https://tunesambiental.com/wp-content/uploads/imagens-do-dia-mundo-panda-completa-um-ano-kuala-lumpur-23082016-023.jpeg)

- armazenar dados com features relevantes, agregações ou sumarizações no formato pandas dataframe

### paradas gold (ouro)

- armazenar dados em formato de tabelas relacionais (sqlite)


---

## começando a brincadeira

clona o repositorio

```
git clone https://github.com/joaopedrov0/bdd2
```


entra na pasta

```
cd bdd2
```

ignore todos os comandos a frente e use esse:

> se esse comando não funcionar, de autorização de execução pra ele: `chmod +x setup.sh`

```
sh setup.sh
```


entra no venv (comando pra linux abaixo)

```linux
source ./venv/bin/activate
```

instala as dependencias

```
pip install -r requirements.txt
```

não esquece de criar o .env por favor, eu deixei um exemplo ali (pode ignorar a parte do mongodb por enquanto, eu nao fiz nada com mongodb ainda)


## DEPOIS QUE TIVER TUDO PRONTO

Pra ativar:

```
docker compose up -d
```

baixa os dado dos cartao

```
python feed_db.py
```

> o `-d` é pra ele deixar tu digitar no terminal depois (detached)

se quiser entrar no container

```
docker exec -it mysql_bdd2 bash
```

> as letras `i` e `t` são flags de configuração, eu não lembro agora oq cada uma faz individualmente, mas o resultado é um terminal interativo dentro do container `mysql_bdd2` com o bash (terminal) rodando pra ti fazer oq quiser la

quando tiver la dentro vc pode rodar

```
mysql -u root -p
```

a senha vc q escolhe (tem que criar o .env)

pra sair do container

```
exit
```

pra derrubar os containers

```
docker compose down
```
