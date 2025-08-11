import pandas as pd
from sodapy import Socrata #conectarse al api de IDEAM
import time
# api ID = 1kp8n8fejbe6edsrsmbxs18wi
# key = cazfog281efdbdsx667kqhjh7z472i1l92wmhsad44sdlg5v0



client = Socrata("www.datos.gov.co", 
                 #app_token = "1kp8n8fejbe6edsrsmbxs18wi",
                 #access_token = "cazfog281efdbdsx667kqhjh7z472i1l92wmhsad44sdlg5v0",
                 None,
                 timeout = 180)


endpoint = "57sv-p2fu"


client.get_metadata(endpoint)

codigo_est = "0048015050"

response = client.get_all(endpoint, where = f'codigoestacion = \"{codigo_est}\"')

#results_df = pd.DataFrame.from_records(response)
#results_df.shape

df_lst = []
while True:
    try:
        df_lst.append(next(response))
    except StopIteration:
        break

results_df  = pd.DataFrame.from_records(df_lst)

results_df["descripcionsensor"].value_counts()

prec = results_df[results_df["descripcionsensor"] == "GPRS - PRECIPITACIÓN"].copy()
prec["fechaobservacion"] = pd.to_datetime(prec["fechaobservacion"])
prec["fechaobservacion"].min()
prec["fechaobservacion"].max()



response_prec = client.get("s54a-sgyg", limit = 50000, offset = 10, where = f'codigoestacion = \"{codigo_est}\"')

#provbar esta parte
all_data = []
limit = 20000
offset = 0

while True:

    print(f"Obteniendo datos para el offset: {offset}")
    try: 
        chunk = client.get("s54a-sgyg", limit=limit, offset=offset,  where = f'codigoestacion = \"{codigo_est}\"')
    except Exception as e:
        # Maneja cualquier excepción y muestra un mensaje de error
        print(f"Algo ha ocurrido: {e}")
        break
    else:
        if not chunk:
            print("No hay más datos.")
            break
        all_data.extend(chunk)
        offset += limit
        time.sleep(63)


full_df = pd.DataFrame.from_records(all_data)

full_df["fechaobservacion"][0]







