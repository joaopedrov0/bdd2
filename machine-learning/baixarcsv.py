import os
import shutil
import kagglehub
import pandas as pd

def main():
    print("Baixando dataset do Kaggle...")
    path = kagglehub.dataset_download("mlg-ulb/creditcardfraud")
    print("Dataset baixado em:", path)

    target_filename = "creditcard.csv"
    target_path = None

    # Encontrar o arquivo dentro do download
    for root, dirs, files in os.walk(path):
        if target_filename in files:
            target_path = os.path.join(root, target_filename)
            break

    if not target_path:
        print("Erro: creditcard.csv não encontrado no dataset.")
        return

    # Copiar para a pasta atual
    local_copy = os.path.join(os.getcwd(), target_filename)
    shutil.copyfile(target_path, local_copy)
    print(f"Arquivo copiado para {local_copy}")

    # Ler CSV
    print("Processando arquivo e renomeando coluna 'Class' para 'class'...")
    df = pd.read_csv(local_copy)

    if "Class" not in df.columns:
        print("Erro: coluna 'Class' não encontrada no CSV.")
        return

    df.rename(columns={"Class": "class"}, inplace=True)

    # Salvar novamente
    df.to_csv(local_copy, index=False)
    print("Coluna renomeada com sucesso.")

    # Renomear arquivo final
    final_name = "credit-card.csv"
    final_path = os.path.join(os.getcwd(), final_name)

    # Se já existir, remover
    if os.path.exists(final_path):
        os.remove(final_path)

    os.rename(local_copy, final_path)
    print(f"Arquivo final gerado: {final_path}")

if __name__ == "__main__":
    main()
