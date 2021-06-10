# - ## Importando bibliotecas

# In[1]:

import pandas as pd
import numpy as np
import platform
import warnings
import ntpath
import sys
import re
from os import listdir
from os.path import join
from unicodedata import normalize
from bmldev.loads import txts_to_pd


# #### Bibliotecas para Linux

# In[2]:


if platform.system() == 'Linux':
    from IPython.core.magic import register_line_cell_magic
    from bmldev.loads import sap_to_df


# - ## Dicionários úteis

# In[3]:


dic_centros = {'104':'CD',
               '102':'Bol',
               '600':'Online',
               '601':'Torquato',
               '602':'Camapuã',
               '603':'Am.Shop',
               '604':'G.Circular',
               '605':'Matriz',
               '606':'Shop.PNegra',
               '607':'Nova Cidade',
               '608':'Millenium',
               '609':'P.Velho',
               '610':'Boa Vista',
               '611':'Itacoatiara',
               '612':'Manauara',
               '613':'Ariquemes',
               '614':'Pres.Figueiredo',
               '615':'Djalma',
               '616':'Ji-Paraná',
               '617':'Ponta Negra',
               '618':'Studio 5',
               '619':'Jatuarana',
               '620':'Avenida',
               '621':'Súmauma',
               '622':'Autazes',
               '623':'Ateíde Teive',
               '624':'Manacapuru'
                }


# In[4]:


dic_mes = {1: 'JAN', 2: 'FEV', 3: 'MAR', 4: 'ABR', 5:'MAIO', 6:'JUN', 7:'JUL', 8:'AGO', 9:'SET', 10:'OUT', 11:'NOV',12:'DEZ'}


# - ## Funções

# - #### Funções utilitárias

# In[5]:


def encontra_tipo(arquivo):
    arq_txts = ['txt', 'csv']
    
    if arquivo is not None:
        if isinstance(arquivo,list):
            arquivo = arquivo[0]
        try:
            tipo = ntpath.basename(arquivo).split('.')[-1]
            tipo = tipo.lower()

            if 'xl' in tipo:
                tipo = 'excel'
            if tipo in arq_txts:
                tipo = 'txt'
        except:
            raise Exception('Não foi possível encontrar o tipo do arquivo')
            
    return tipo

def arquivo_valido(arq):
    return '~lock' not in arq and '~$' not in arq

def warning(mensagem):
    warnings.warn(mensagem, UserWarning)

def encerra(mensagem=''):
    print('{}'.format(mensagem))
    input('Pressione enter para sair.')
#     sys.exit()


# In[6]:


def str_para_numero(text):
    try:
        # First we return None if we don't have something in the text:
        if text is None:
            return None
        if isinstance(text, int) or isinstance(text, float):
            return text
        text = text.strip()
        if text == "":
            return None
        # Next we get the first "[0-9,. ]+":
        n = re.search("-?[0-9]*([,. ]?[0-9]+)+", text).group(0)
        n = n.strip()
        if not re.match(".*[0-9]+.*", text):
            return None
        # Then we cut to keep only 2 symbols:
        while " " in n and "," in n and "." in n:
            index = max(n.rfind(','), n.rfind(' '), n.rfind('.'))
            n = n[0:index]
        n = n.strip()
        # We count the number of symbols:
        symbolsCount = 0
        for current in [" ", ",", "."]:
            if current in n:
                symbolsCount += 1
        # If we don't have any symbol, we do nothing:
        if symbolsCount == 0:
            pass
        # With one symbol:
        elif symbolsCount == 1:
            # If this is a space, we just remove all:
            if " " in n:
                n = n.replace(" ", "")
            # Else we set it as a "." if one occurence, or remove it:
            else:
                theSymbol = "," if "," in n else "."
                if n.count(theSymbol) > 1:
                    n = n.replace(theSymbol, "")
                else:
                    n = n.replace(theSymbol, ".")
        else:
            # Now replace symbols so the right symbol is "." and all left are "":
            rightSymbolIndex = max(n.rfind(','), n.rfind(' '), n.rfind('.'))
            rightSymbol = n[rightSymbolIndex:rightSymbolIndex+1]
            if rightSymbol == " ":
                return parseNumber(n.replace(" ", "_"))
            n = n.replace(rightSymbol, "R")
            leftSymbolIndex = max(n.rfind(','), n.rfind(' '), n.rfind('.'))
            leftSymbol = n[leftSymbolIndex:leftSymbolIndex+1]
            n = n.replace(leftSymbol, "L")
            n = n.replace("L", "")
            n = n.replace("R", ".")
        # And we cast the text to float or int:
        n = float(n)
        if n.is_integer():
            return int(n)
        else:
            return n
    except: pass
    return None


# In[7]:


def str_list_para_num(lista):
    return [str_para_numero(string) for string in lista]


# In[8]:


if platform.system() == 'Linux':
    @register_line_cell_magic
    def tentativa(linha, celula):
        try:
            exec(celula)
        except Exception as e:
            print(e)
            print('Em:')
            print(celula)
            input('Pressione enter para sair')


# - #### Funções de check de dados

# In[9]:


def verifica_nulos(df, print_cols=True, return_bool=True):
    nulos=False
    
    for col in list(df.columns):
        df_aux = df[df[col].isna()]
        
        if df_aux.shape[0] > 0:
            nulos=True
            if print_cols:
                print('Há {} nulos na coluna: {}'.format(df_aux.shape[0], col))
                
    if return_bool:
        return nulos

def verifica_brancos(df, print_cols=True, return_bool=True):
    brancos = False
    df = df.select_dtypes(include='object')
    
    for col in list(df.columns):
        df_aux = df[df[col] == '']
        
        if df_aux.shape[0] > 0:
            brancos=True
            if print_cols:
                print('Há {} brancos na coluna: {}'.format(df_aux.shape[0], col))
                
    if return_bool:
        return brancos
            
def verifica_duplicadas(df, print_cols=True, return_bool=True):
    duplicadas = False
    for col in list(df.columns):
        df_aux = df[df[col].duplicated()]
        
        if df_aux.shape[0] > 0:
            duplicadas = True
            if print_cols:
                print('Há duplicados na coluna: {}'.format(col))
                
    if return_bool:
        return duplicadas


# - #### Funções de tratamento

# In[10]:


def converte_tipos_colunas(df, dic_cols_tipos, sobrescreve=False):
    if not sobrescreve:
        df = df.copy()
    lst_chaves = [tupla for tupla in dic_cols_tipos.items()]
    for tupla in lst_chaves:
        col,tipo = tupla
        if col not in df.columns:
            raise Exception('Coluna {} não encontrada'.format(col))
        try:
            if tipo == 'data':
                df[col] = pd.to_datetime(df[col], dayfirst=True)
            else:
                df[col] = df[col].astype(tipo)
        except Exception as e:
            warning("Não foi possível converter a coluna '{}', para {}\n{}".format(col, tipo, e))
            df[col] = df[col]
        
    if not sobrescreve:    
        return df


# In[11]:


def branco_para_nan(df, branco=['', 'nan'], sobrescreve=False):
#     branco=['', 'nan']    
    if not sobrescreve:
        df = df.copy()
    for col in list(df.select_dtypes(include='object').columns):
        df[col] = df[col].astype(str)
        df[col] = df[col].str.strip()
        df[col] = df[col].apply(lambda nome: np.nan if nome in branco else nome)
    if not sobrescreve:
        return df


# In[12]:


def remove_acentos(txt):
    return normalize('NFKD', str(txt)).encode('ASCII', 'ignore').decode('ASCII')

def normaliza(txt):
    return str(txt).strip().lower().replace(' ','_').replace('.','')

def normaliza_colunas(cols):
    colunas = []
    try:
        cols=cols.tolist()
    except:
        return cols

    for col in cols:
        try:
            colunas.append(remove_acentos(normaliza(col)))
        except:
            colunas.append(col)
            
    return colunas

def renomeia_colunas(colunas):
    return [(str(col)[0].upper() + str(col)[1:]).replace('_',' ') for col in colunas]


# - #### Funções auxiliares

# In[13]:


def busca_arquivos(diretorio=None, palavra_chave=None, multiplas_bases=False):
    caminho=[]
    for arquivo in listdir(diretorio):
        if re.search(palavra_chave, remove_acentos(arquivo), flags=re.IGNORECASE) and arquivo_valido(arquivo):
            if diretorio:
                caminho.append(join(diretorio,arquivo))
            else:
                caminho.append(arquivo)
                
    if len(caminho) == 0:
        raise Exception('Palavra chave não encontrada: {}'.format(str(palavra_chave)))
    
    if multiplas_bases:
        return caminho
        
    elif len(caminho)>1:
        warning("Verifique sua palavra chave: {}, a mesma se aplica a mais de uma base".format(str(palavra_chave)))
        
    return caminho[0]

def leitura_dic_bases(dic_bases, normaliza=True, Linux=False):
    dfs={}
    for base in dic_bases:
        print ("Lendo base {}...".format(base), end='') 
        
        try:
            if dic_bases[base]['tipo'] == 'excel':
                dfs[base] = pd.read_excel(dic_bases[base]['caminho'], dic_bases[base]['sheet'])
             
            if dic_bases[base]['tipo'] == 'txt':
                if Linux:
                    dfs[base] = sap_to_df(dic_bases[base]['caminho'], dic_bases[base]['colunas'], delimiter=dic_bases[base]['sep'])
                else:
                    dfs[base] = txts_to_pd(dic_bases[base]['caminho'], dic_bases[base]['colunas'], delimiter=dic_bases[base]['sep'])
                                
            if normaliza:
                dfs[base].columns = normaliza_colunas(dfs[base].columns)
                
            if dfs[base].shape[0] == 0:
#                 encerra('Verificar a quantidade de colunas da base')
                raise Exception('Número de colunas informado não é compatível com a base')
            print('ok')
            
        except Exception as e:
            encerra('Erro na base: {} \n {}'.format(base,e))
            break            
    return dfs

def preenchedor_lista_parametros_bases(palavras_chave, tipo, preenchedor, lista_para_preencher, diretorio='bases'):
    
    cont_arq = len([chave for chave in palavras_chave if encontra_tipo(busca_arquivos(diretorio, chave, multiplas_bases=True)) == tipo])
    
    if lista_para_preencher:
        if len(lista_para_preencher) < cont_arq:
            diff_tam = cont_arq - len(lista_para_preencher)
            lista_para_preencher = lista_para_preencher + [preenchedor for item in range(diff_tam)]
            
        return lista_para_preencher
    
    else:
        lista_para_preencher = [preenchedor for item in range(cont_arq)]
        return lista_para_preencher

def cria_elemento(palavra_chave, diretorio=None, col=None, sheet_name=None, separador=None, ignora_linha=None, multiplas_bases_txt=False):
    dic = {}
    base_caminho = busca_arquivos(diretorio=diretorio, palavra_chave=palavra_chave, multiplas_bases=multiplas_bases_txt) 
    
    if not isinstance(base_caminho, list):
        base_caminho = [base_caminho]
     
    dic['caminho'] = base_caminho  
    dic['tipo'] = encontra_tipo(dic['caminho'])
  
    if dic['tipo'] == 'txt':
        dic['colunas'] = col
        dic['sep'] = separador
        
    if dic['tipo'] == 'excel':
        dic['caminho'] = base_caminho[0]
        dic['sheet'] = sheet_name
#         dic['ignora_linha_col'] = ignora_linha

    return dic


# - #### Função cria_dfs

# In[14]:


def cria_dfs(nomes_bases=[], palavras_chave=[], cols=None, sheet_names=None, sep_txt=None, diretorio='bases', multiplas_bases_txt=False, normaliza_colunas=True, debug=False):
    '''
    Cria um dicionário, onde cada chave dá acesso a um dataframe.
    Para leitura de bases em .txt, é usada a biblioteca bmldev.loads
    Para leitura de bases em .xls(x), é usado a biblioteca pandas

    Parametros
    ----------
    nomes_bases: uma lista de strings, onde cada elemento será a chave de acesso do dataframe, se associando aos outros parametros pelo indice

    palavras_chave: uma lista de strings, onde cada elemento será a palavra chave para buscar o arquivo a ser lido, no diretorio definido, se associando aos outros parametros pelo indice

    cols: um lista de inteiros, onde cada elemento indica o número de colunas de uma base .txt, se associando apenas as bases .txt e aos outros parametros por indice

    sheet_names: uma lista de strings, onde cada elemento indica a sheet a ser considerada na leitura de uma base .xls(x),se associando apenas as bases .xls(x) e aos outros parametos por indice.

    Obs: para ler mais de uma sheet de um mesmo arquivo, adicione uma novo elemento em nomes_bases, repetindo a palavra chave e adicionando um elemento em sheet_names, especificando a outra sheet

    sep_txt: uma lista de strings, onde cad elemento indica o separador a ser usado para uma base .txt(.csv), se associando apenas as bases .txt e aos outros parametros por indice

    ignora_linha: Implementada, mas não ativa, falar com Leo sobre

    diretorio: uma string, contendo o nome da pasta a partir do dirertorio do programa. Nesse diretorio serão procurados os arquivos, utilizando os elementos da lista palavras_chave
    *Por padrão: 'bases'
    
    multiplas_bases_txt: Uma lista de booleanos, onde cada elemento indica se uma base .txt está dividida em várias partes e concatena-as na leitura, se associando apenas as bases .txt e aos outros parametros por indice
    
    normaliza_colunas: um booleano.
        *Por padrão: True
        *Casos:
            True:
            Remove caracteres especiais dos nomes das colunas, substitui espaços por underline e letras em uppercase para lowercase
            False:
            Faz nada ksk

    debug: um booleano, caso verdadeiro retorna um dicionário contendo os dados de leitura da bases(não retorna a bases lidas)
    
    Exemplo de uso
    ----------

    Reafirmando, como anteriormente explicado,os argumentos recebidos(listas), se associam pelo índice.
    No exemplo abaixo, a função irá usar as palavras chave para buscar arquivos na pasta 'bases_2019',
    O elemento 'vendas' de nomes_bases, será a chave de acesso do dicionário dfs, para o arquivo lido a partir da palavra_chave 'janeiro', que tem 7 colunas. É uma base .txt dividida em mais de 1 arquivo
    O elemento 'dados' de nomes_bases, será a chave de acesso do dicionário dfs, para o arquivo lido a partir da palavra_chave 'relatorio', na sheet 'Cadastro'. É uma base .xls(x)


    dfs = cria_dfs(
            diretorio = 'bases_2019',
            nomes_bases = ['vendas','dados'],
            palavras_chave = ['janeiro','relatorio'],
            cols = [7],
            sheet_names = ['Cadastro'],
            multiplas_bases_txt = [True]
            )
    '''
    #Verifica o S.O.
    linux = True if platform.system() == 'Linux' else False 

    dic_bases={} #Dicionário principal de bases
    cont_txt = 0 #Contador para atribuir o n° de colunas aos arquvios .txt
    cont_xls = 0 #Contador para atribuir o n° de colunas aos arquvios .xls(x)
    
    #Preenchendo listas relacionadas aos .xls(x), para iteração 
    sheet_names = preenchedor_lista_parametros_bases(palavras_chave, 'excel', 0, sheet_names, diretorio=diretorio)
#     ignora_linha = preenchedor_lista_parametros_bases(palavras_chave, 'excel', 0, ignora_linha, diretorio=diretorio)
    
    #Preenchendo listas relacionadas aos .txt, para iteração
    sep_txt = preenchedor_lista_parametros_bases(palavras_chave, 'txt', '|', sep_txt, diretorio=diretorio)
    multiplas_bases_txt = preenchedor_lista_parametros_bases(palavras_chave, 'txt', False, multiplas_bases_txt, diretorio=diretorio)   
    
    #Verifica se existe um nome para cada palavra_chave
    if len(nomes_bases) == len(palavras_chave):
        
        #Itera pelo tamanho da lista de palavras_chaves
        for i in range(len(palavras_chave)):
            
            #Define o tipo do arquivo
            tipo = encontra_tipo(busca_arquivos(diretorio=diretorio, palavra_chave=palavras_chave[i], multiplas_bases=True))
            if tipo:
                #Arquivos .txt
                if tipo == 'txt':
                    if cont_txt < len(cols):
                        #Criando dicionário com as informações da base
                        aux = cria_elemento(palavra_chave = palavras_chave[i],
                                            diretorio = diretorio,
                                            col = cols[cont_txt],
                                            separador=sep_txt[cont_txt],
                                            multiplas_bases_txt = multiplas_bases_txt[cont_txt])
                        
                        #Controlando iteração das bases .txt
                        cont_txt+=1
                        
                    else:
                        print('Erro nas informações do n° de colunas')

                #Arquivos .xls(x)        
                if tipo == 'excel':
                    #Criando dicionário com as informações da base
                    aux = cria_elemento(palavra_chave = palavras_chave[i],
                                        diretorio = diretorio,
                                        sheet_name = sheet_names[cont_xls])
#                                         ignora_linha_col=ignora_linha_col[cont_xls])
                    
                    #Controlando iteração das bases .xls(x)
                    cont_xls+=1

                #Adicina dicinário criado ao dicionário principal    
                dic_bases[nomes_bases[i]] = aux

    else:
         raise Exception("Erro, dados de entrada incorretos, quantidade de nomes precisa ser igual a de palavras chaves")
            
    if debug:
        return dic_bases
    #Chama a função de leitura de bases a partir de dicionários, usando o dicionári0 principal de bases criado
    return leitura_dic_bases(dic_bases, normaliza=normaliza_colunas, Linux=linux)


# - #### Função le_base

# In[15]:


def le_base(diretorio=None, palavra_chave=None, sheet_name=None, skiprows=None, cols=None, separador=None, multiplas_bases_txt=False, normaliza=True):
    
    caminho = busca_arquivos(diretorio, palavra_chave, multiplas_bases_txt)
    tipo = encontra_tipo(caminho)
    linux = True if platform.system()=='Linux' else False
    try:           
        if tipo == 'txt':
            if linux:
                base = sap_to_df(caminho, cols, delimiter=separador)
            else:
                if not isinstance(caminho, list):
                    caminho = [caminho]
                base = txts_to_pd(caminho, cols, delimiter=separador)

        if tipo == 'excel':
            base = pd.read_excel(caminho, sheet_name=sheet_name, skiprows=skiprows)

        if normaliza:
            base.columns = normaliza_colunas(base.columns)
            
    except Exception as e:        
        print('Erro ao ler base: ')
        raise(e)

    return base

def le_base_excel(diretorio=None, palavra_chave=None, sheet_name=0, skiprows=0, normaliza=True):
    
    base = le_base(diretorio=diretorio, palavra_chave=palavra_chave, sheet_name=sheet_name, skiprows=skiprows, normaliza=normaliza)
        
    return base

def le_base_txt(diretorio=None, palavra_chave=None, cols=None, multiplas_bases_txt=False, separador='|', normaliza=True): 
    
    base = le_base(diretorio=diretorio, palavra_chave=palavra_chave, cols=cols, multiplas_bases_txt=multiplas_bases_txt, separador=separador, normaliza=normaliza)
        
    return base
