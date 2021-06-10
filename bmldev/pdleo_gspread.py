import pandas as pd

##Bibliotecas para conexão com a API do Google
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Biblioteca para envio e recebimento de dados via Python
import pygsheets

# pdleo
from bmldev.pdleo import busca_arquivos, normaliza_colunas, normaliza, remove_acentos, str_list_para_num

# Funções utilitárias

def float_to_str_df(df, sobrescreve=False):
    if not sobrescreve:
        df=df.copy()
    
    ls_cols_float = list(df.select_dtypes(exclude=['int64', 'object', int]).columns)

    for col in df[ls_cols_float].columns:
        df[col] = round(df[col],2) 
        df[col] = df[col].astype(str)
        df[col] = df[col].str.replace('.', ',')
        
    if not sobrescreve:
        return df

# Funções import, export

def exporta_df_gspread(dir_client_json, df, arquivo, sheet, sobrescreve=False):
    client = dir_client_json
    
    # Autorização
    try:
        credencial = pygsheets.authorize(service_file=client)
    except:
        raise Exception('Client_id inválido')
    
    # Abertura spreadsheet
    try:
        arq = credencial.open(arquivo)
    except:
        raise Exception(f'Arquivo {arquivo} não encontrado')
    
    # Definição de sheets
    if normaliza(sheet) not in [normaliza(sh.title) for sh in arq.worksheets()] and not sobrescreve:
        sh_writer = arq.add_worksheet(title=sheet)
    else:
        sh_writer = arq.worksheet_by_title(sheet)
    
    # Tratamento de floats
    #float_to_str_df(df, sobrescreve=True)
    # Upload df
    try:
        if sobrescreve:
            sh_writer.clear()
            sh_writer.resize(rows=1, cols=1)
            sh_writer.set_dataframe(df, "A1", extend=True)
        else:    
            int_linha = len(sh_writer.get_all_records())+2

            if int_linha==2:
                sh_writer.set_dataframe(df, "A1", extend=True)
            else:
                sh_writer.set_dataframe(df, f"A{int_linha}", copy_head=False, extend=True)
    except:
        raise Exception('Erro no upload do dataframe')
    
    return True

def importa_df_gspread(dir_client_json, arquivo, sheet):
    client = dir_client_json
    
    try:
        credencial = pygsheets.authorize(service_file=client)
    except:
        raise Exception('Client_id inválido')
    
    try:
        arq = credencial.open(arquivo)
    except:
        raise Exception(f'Arquivo {arquivo} não encontrado')
    
    try:
        r_df = arq.worksheet_by_title(sheet).get_as_df()
        r_df.columns = normaliza_colunas(r_df.columns)
    except:
        raise Exception(f'Sheet {sheet} não encontrada')

    return r_df