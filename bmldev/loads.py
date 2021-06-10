import pandas as pd
import re
import platform
from io import BytesIO

# #### bmldev.loads

# In[5]:


def __char_validation(char):
    invalid = ['', '\n', '\t']
    if char in invalid:
        return False
    return True

def __line_validation(line, qtd_columns, delimiter):
    line_data = line.split(delimiter)
    line_data = list(map(str.strip, line_data))
    len_line = len(line_data)
    if (len_line == qtd_columns): 
        return line_data.copy()
    
    if (len_line == qtd_columns+2): 
        if (not __char_validation(line_data[0]) and not __char_validation(line_data[-1])):
            return line_data[1:-1]
    
    raise ValueError('Quantidade inválida de colunas na linha')

def __str_to_float(text, ind_cut, negative=False):
    pref = text[:ind_cut].replace('.', '').replace(',', '')
    suf = text[ind_cut:].replace(',', '.')
    if (negative):
        return float(pref + suf) * -1
    else:
        return float(pref + suf)

def __to_numeric(val):
    if (re.search(r'^(\d\d)?\d\d\.\d\d?\.\d\d(\d\d)?$', val)): # verificação de padrão de data (xx.xx.xxxx)
        return val

    found_re = re.search(r'[\.]\d{3}$', val) # verfica *.xxx (positivo)
    if (found_re):
        try:
            val.replace('.', '')
            return int(val.replace('.', ''))
        except:
            return val

    found_re = re.search(r'[\.]\d{3}-$', val) # verfica *.xxx (negativo)
    if (found_re):
        try:
            val.replace('.', '')
            return int(val[:-1].replace('.', '')) * -1
        except:
            return val
    
    found_re = re.search(r'[\.\,]\d+$', val) # verfica *.xx ou *,xx (positivo)
    if (found_re):
        try:
            return __str_to_float(val, found_re.span()[0])
        except:
            return val

    found_re = re.search(r'[\.\,]\d+-$', val) # verfica *.xx ou *,xx (negativo)
    if (found_re):
        try:
            return __str_to_float(val[:-1], found_re.span()[0], True)
        except:
            return val

    if (re.search(r'^\d+-$', val)):
        return (int(val[:-1])*-1)

    if (re.search(r'^\d+$', val)):
        return (int(val))

    return val 

def __back_to_blank(text, begin):
    for ind in range(begin,-1,-1):
        if (text[ind-1] == ' '): return(ind)
    return 0

def __end_column(text, begin):
    ind = text[begin:].find('   ')
    if (ind < 0):  return ind
    ind = ind + 3
    for i in range(ind + begin, len(text)):
        if (text[i] != ' '): return i
    return None

def __get_begin_end(text, column):
    begin = text.find(column)
    if (begin < 0): return None   # retorno caso nao encontre a coluna
    end = __end_column(text, begin)
    if not(end): return None      # retorno caso encontre algum erro no end
    return (begin, end)

def __conditions(header, line):
    if (len(line) < len(header)): return False
    if (re.search(r'^-+$', line)): return False
    if (line == header): return False
    return True

def txts_to_pd(arquivos, qtd_columns, has_header=True, encoding='latin-1', delimiter='|', cols_names=None):

    header_names = []

    dic = {i:[] for i in range(qtd_columns)}
    
    for arquivo in arquivos:
        arq = arquivo.readlines() if type(arquivo) == BytesIO else open(arquivo, "r", encoding=encoding)
        for line in arq:
            line = line.decode(encoding) if type(arquivo) == BytesIO else line
            done = False
            try:
                data = __line_validation(line, qtd_columns, delimiter)
                done = True
            except:
                done = False

            if (done):
                for i in range(len(data)):
                    data[i] = __to_numeric(data[i])
                if (not header_names):
                    if (has_header):
                        # print(data)
                        header_names = data
                else:
                    if (data != header_names):
                        for i in range(qtd_columns):
                            if (isinstance(data[i], str)):
                                dic[i].append(data[i].strip())
                            else:
                                dic[i].append(data[i])
                
    df = pd.DataFrame.from_dict(dic)
    # print(header_names)
    if (len(header_names) == qtd_columns) & (cols_names == None):
        df.columns = header_names
    elif (cols_names != None):
        df.columns = cols_names
    return df

def zvlike_to_df(columns, file, skip=6, encoding="latin-1"):
    coord = {}
    df = dict([(col, []) for col in columns])
    header = ''
    cont = 0                          # force break
    with open(file, "r", encoding=encoding) as slar:
        for line in slar.readlines()[skip:]:
            if not (header): 
                header = line
                for col in columns:
                    coord[col] = __get_begin_end(header, col)
            else:
                if (__conditions(header, line)):
                    for col in columns:
                        value = line[__back_to_blank(line, coord[col][0]):coord[col][1]].strip()
                        df[col].append(__to_numeric(value))
            cont += 1                 # force break
            if (cont > 75): break      # force break

    return pd.DataFrame.from_dict(df)

def get_lineData(line, n_cols, delimiter):
    line_data = line[1:-2].split(delimiter)
    line_data = list(map(str.strip, line_data))
    len_line = len(line_data)
    
    if (len_line == n_cols):
        # if (not __char_validation(line_data[0]) and not __char_validation(line_data[-1])):
        return line_data
    
    return None

def line_to_dict(lines, n_cols, delimiter):
    dic = {i:[] for i in range(n_cols)}
    if type(lines) != list:
        lines = [lines]

    for line in lines:
        # print(line)
        data = get_lineData(line, n_cols, delimiter)
        
        if data:
            for i in range(n_cols):
                data[i] = __to_numeric(data[i])
                if (isinstance(data[i], str)):
                    dic[i].append(data[i].strip())
                else:
                    dic[i].append(data[i])
    return dic

def file_to_df(path, n_cols, delimiter, encoding, slice_size=10):
    lines = path.readlines() if type(path) == BytesIO else open(path, "r", encoding=encoding)
    dic_all = {i:[] for i in range(n_cols)}
    header_names = None
    for line in lines:
        line = line.decode(encoding) if type(path) == BytesIO else line

        data = get_lineData(line, n_cols, delimiter)
        if data:
            if not header_names:
                header_names = data.copy()
            elif data != header_names:
                ret = line_to_dict(line, n_cols, delimiter)
                
                for key, vals in ret.items():
                    for val in vals:
                        dic_all[key].append(val)


    df = pd.DataFrame.from_dict(dic_all)
    df.columns = header_names
    return df

if platform.system() == 'Linux':
    from concurrent.futures import ProcessPoolExecutor, as_completed
    
    def read_files(files, n_cols, delimiter, encoding):
        futures = []
        result = pd.DataFrame()
        with ProcessPoolExecutor(max_workers=4) as executor:
            for path in files:
                # futures.append(executor.submit(txts_to_pd, [path], n_cols))
                futures.append(executor.submit(file_to_df, path, n_cols, delimiter, encoding))

            for future in as_completed(futures):
                ret = future.result()
                result = pd.concat([result, ret]).reset_index(drop=True)

        return result


    def sap_to_df(paths, n_cols, has_header=True, encoding='latin-1', delimiter='|', cols_names=None):

        if type(paths) != list:
            paths = [paths]

        df = read_files(paths, n_cols, delimiter=delimiter, encoding=encoding)

        if cols_names:
            df.columns = cols_names

        return df

