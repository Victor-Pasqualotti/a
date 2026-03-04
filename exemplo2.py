import concurrent.futures
import time
import random
import pandas as pd
import threading

start_time = time.time()

# Simula a chamada da IA
def chama_ia(i, prompts):
    print(f'[{threading.current_thread().name}]', f'Recebi index {i} e lista: {prompts}')
    print('Tempo ate agora: {:.2f} segundos \n ------------------------'.format(time.time() - start_time))
    time.sleep(random.randint(1, 10))  # Simula o tempo de processamento da IA
    return i, prompts

# Simula publico
df = pd.DataFrame.from_dict({
    'primeiro_argumento' : [str(i) for i in range(100)],
    'segundo_argumento'  : [str(i) for i in range(100,200)],
})
#print(df.head())

# Lista de prompts
lista_prompts = [
    f"Argumentos: {row['primeiro_argumento']}, {row['segundo_argumento']}" for index, row in df.iterrows()
]

# Execução em loop, usando Batch de prompts
Batch = 7
MaxWorkers = 4
respostas_futuras = list() # Lista de respostas futuras
with concurrent.futures.ThreadPoolExecutor(max_workers=MaxWorkers) as executor:

    # A MainThread envia os prompts em lotes para a IA
    # Faz isso enviando prompts para serem executados em paralelo, em cada trhead.
    # E armazena a resposta futura (futures) para cada lote de prompts enviado.
    for start in range(0, len(lista_prompts), Batch):
        end = start + Batch
        batch_prompts = lista_prompts[start:end]
        # Envia o lote de prompts para a IA e armazena a resposta futura
        print('Vou requisitar o processamento do lote: ', start)
        respostas_futuras.append(executor.submit(chama_ia, start, batch_prompts))
        
    # A MainThread coleta os resultados à medida que ficam prontos
    for resposta_concluida in concurrent.futures.as_completed(respostas_futuras):
        i, prompts = resposta_concluida.result()
        print(f'\n[{threading.current_thread().name}]', f'Resposta {i} concluída, processando resultado...')
        end_idx = i + len(prompts) - 1
        df.loc[i:end_idx, 'resposta_ia'] = [p for p in prompts]

print(df.head(10))
print(df.tail(10))