import time
import numpy as np
import pandas as pd
from openai import OpenAI
from openai import RateLimitError, InternalServerError
from local_paths import your_local_save_fold
from dotenv import load_dotenv
import os




load_dotenv()
api_key = os.getenv("API_KEY")
client = OpenAI(api_key=api_key)

# --- Lecture des commentaires ---
df = pd.read_csv("bdd.csv")
batch_size = 50
comm_id = 'q35'

df_col_commentaire_haineux = df.copy()
df_comm = df[['uid', comm_id]].dropna(subset=[comm_id])
thresh = 0.55
commentaires_haineux_uid = pd.DataFrame(columns=['uid', 'commentaire', 'max_prob', 'key_max_prob'])

def moderer_batch(commentaires):
    """Appelle l'API avec retry automatique"""
    backoff = 2  # backoff initial si pas de Retry-After
    while True:
        try:
            response = client.moderations.create(
                model="omni-moderation-latest",
                input=commentaires
            )
            return response
        except RateLimitError as e:
            retry_after = None
            if hasattr(e, "response") and e.response is not None:
                retry_after = e.response.headers.get("Retry-After")
            if retry_after:
                wait_time = int(retry_after)
                print(f"Rate limit hit – attendre {wait_time}s")
            else:
                wait_time = backoff
                print(f"Rate limit hit – attendre {wait_time}s (backoff)")
                backoff = min(backoff * 2, 60)  # max 1 min
            time.sleep(wait_time)
        except InternalServerError as e:
            print('Internal server error')
            time.sleep(2)


# --- Boucle sur les batchs ---
#for i in range(5):
for i in range(44650//50,len(df_comm)//batch_size + 1):
    print('batch', i*batch_size)
    if i < len(df_comm)//batch_size:
        sub_df = df_comm[i*batch_size:(i+1)*batch_size]
    else:
        sub_df = df_comm[i*batch_size:]
    comm_sub_df = list(sub_df[comm_id])

    responses = moderer_batch(comm_sub_df)

    for j, commentaire in enumerate(comm_sub_df):
        dict_scores = responses.results[j].category_scores.__dict__
        print('dict scores', dict_scores)
        commentaire_haineux = False
        max_val = 0
        key_max_val = ''
        for elt in dict_scores.keys():
            val = dict_scores[elt]
            if val > max_val:
                max_val = val
                key_max_val = elt
        if max_val > thresh:
            commentaire_haineux = True

        uid = sub_df.iloc[j]['uid']
        df_col_commentaire_haineux.loc[df['uid'] == uid, 'Message haineux'] = commentaire_haineux
        if commentaire_haineux:
            #df_col_commentaire_haineux.loc[df['uid'] == uid, comm_id] = np.nan
            new_row = pd.DataFrame({
                'uid': [uid],
                'commentaire': [commentaire.replace("\n", "")],
                'max_prob': [max_val],
                'key_max_prob': [key_max_val]
            })
            commentaires_haineux_uid = pd.concat([commentaires_haineux_uid, new_row], ignore_index=True)
            print(comm_sub_df[j])
            print('max val', max_val)
            print('key max val', key_max_val)
        commentaires_haineux_uid.to_csv(f'{your_local_save_fold}/commentaires_haineux_6.csv', index=False)

# --- Sauvegarde ---
df_col_commentaire_haineux.to_csv(f'{your_local_save_fold}/bdd_comm_commentaires_haineux.csv', index=False)


