import concurrent.futures
import time
import random
import pandas as pd

df = pd.DataFrame.from_dict({
    'lista_prompts' : [str(i) for i in range(100)],})

Batch = 7

def chama_ia(i, prompts):
    print(f'Recebi index {i} e lista: {prompts}')
    return i, prompts

lista_prompts = [
    row['lista_prompts'] for index, row in df.iterrows()
]

futures = list()
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:

    for start in range(0, len(lista_prompts), Batch):
        end = start + Batch
        batch_prompts = lista_prompts[start:end]
        futures.append(executor.submit(chama_ia, start, batch_prompts))
        
    # 2. A MainThread coleta os resultados à medida que ficam prontos
    for futuro_concluido in concurrent.futures.as_completed(futures):
        i, prompts = futuro_concluido.result()
        end_idx = i + len(prompts) - 1
        df.loc[i:end_idx, 'resposta_ia'] = [p for p in prompts]

print(df.head())
print(df.tail())