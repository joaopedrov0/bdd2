import os
import shutil
import kagglehub
import pandas as pd

def main():
    print("Baixando dataset do Kaggle...")
    path = kagglehub.dataset_download("mlg-ulb/creditcardfraud")
    print("Dataset baixado em:", path)

    target_filename = "credit-card.csv"
    target_path = None

    # Procurar arquivo dentro do download
    for root, dirs, files in os.walk(path):
        if target_filename in files:
            target_path = os.path.join(root, target_filename)
            break

    if not target_path:
        print("Erro: creditcard.csv não encontrado no dataset.")
        return

    # Copiar para a pasta atual
    destination = os.path.join(os.getcwd(), target_filename)
    shutil.copyfile(target_path, destination)
    print(f"Arquivo copiado para {destination}")

    # Ler CSV e renomear coluna
    print("Renomeando coluna 'Class' para 'class'...")
    df = pd.read_csv(destination)

    if "Class" not in df.columns:
        print("Erro: coluna 'Class' não existe no arquivo.")
        return

    df.rename(columns={"Class": "class"}, inplace=True)

    # Salvar novamente com a coluna renomeada
    df.to_csv(destination, index=False)
    print("Coluna renomeada e arquivo salvo com sucesso.")

if __name__ == "__main__":
    main()

