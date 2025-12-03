"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import pandas as pd
import os
from pathlib import Path
import shutil
def create_folder(path):
    """
    Ensure an empty directory at the given path.
    If the directory exists it is removed (recursively) and recreated.

    Parameters:
        path (str | Path): Directory path to (re)create.

    Returns:
        Path: The created empty directory.

    Raises:
        OSError: If removal or creation fails.
    """
    p=Path(path)
    try:
        if p.exists():
            shutil.rmtree(p)
        p.mkdir(parents=True)
    except OSError as e:
        raise OSError(f"Cannot create folder '{path}' : {e}") from e
    return p

def read_zip_csv(path):
    """
    """
    p=Path(path)
    df=pd.read_csv(path,compression="zip")
    df=cleaning(df)
    return df

def cleaning(df:pd.DataFrame):
    df["job"] = (
                df["job"]
                .str.replace(".", "", regex=False)
                .str.replace("-", "_", regex=False))
    
    df['education']=(
                    df['education']
                    .str.replace(".", "_", regex=False)
                    .replace("unknown",pd.NA))
    #binary columns
    df['credit_default']=df['credit_default'].eq("yes").astype(int)
    df['mortgage']=df['mortgage'].eq("yes").astype(int)
    df['previous_outcome']=df['previous_outcome'].eq("success").astype(int)
    df['campaign_outcome']=df['campaign_outcome'].eq("yes").astype(int)


    df['last_contact_date']=pd.to_datetime(
            "2022-"+df['month'].astype(str)+"-"+df['day'].astype(str),
            format="%Y-%b-%d").dt.strftime("%Y-%m-%d")
    return df

def read_files(path:str):
    dfs=[]
    for file in os.listdir(path):
        file_path=os.path.join(path,file)
        dfs.append(read_zip_csv(file_path))
    return dfs
def merge_dfs(dfs:list[pd.DataFrame]):
    return pd.concat(dfs,ignore_index=True)

def split_df(df:pd.DataFrame):
    
    df_client=df[['client_id','age','job','marital','education',
                  'credit_default','mortgage']]
    df_campaing=df[['client_id', 'number_contacts', 'contact_duration', 
                    'previous_campaign_contacts', 'previous_outcome', 
                    'campaign_outcome', 'last_contact_date']]
    df_economics=df[['client_id','cons_price_idx','euribor_three_months']]
    return (df_client,df_campaing,df_economics)
def save_df(df:pd.DataFrame,dir:str,name:str):
    file_path=os.path.join(dir,name)
    if os.path.isfile(file_path):
        os.remove(file_path)
    df.to_csv(file_path,index=False)

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    input="files/input"
    output="files/output"
    create_folder(output)
    dfs=read_files(input)
    df=merge_dfs(dfs)
    dfs=split_df(df)
    names=["client.csv","campaign.csv","economics.csv"]
    for name,df in zip(names,dfs):
        save_df(df,output,name)


if __name__ == "__main__":
    clean_campaign_data()





