import os

import pandas as pd
import numpy as np

from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import time

from fuzzywuzzy import fuzz
from fuzzywuzzy import process

import requests

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.ticker import AutoMinorLocator, MultipleLocator
from matplotlib import colors
from matplotlib import cm
from matplotlib.colors import ListedColormap


def get_excel_from_tcd(auth_token: str, url: str, folders_: 1, path=''):
    '''
    This function downloads the excel from the TCD prod server.

    Args:

        - auth_token: Authentication token for the TCD server.
    '''
    folders = []
    if type(folders_) is not list:
        folders.append(folders_)
    else:
        folders = folders_.copy()

    if (path != '') and (not os.path.exists(path)):
        try:
            os.mkdir(path)
        except OSError:
            print ("Creation of the directory %s failed" % path)
        else:
            print ("Successfully created the directory %s " % path)

    for folder_id in folders:
        
        EXCEL_FILE = 'caso-'+str(folder_id)+'.xlsx'
        if os.path.exists(path+EXCEL_FILE):

            print(f'WARNING: requested file { str(folder_id) } already exists, not downloading again.')
            continue

        print(f'Downoading data [ {str(folder_id)} ] from TCD site ...')

        querystring = {"folder":str(folder_id),"format":"xlsx"}

        payload = ""
        headers = {'Authorization': f'Token {auth_token}'}

        response = requests.request("GET", url, data=payload,
                                    headers=headers, params=querystring)

        if response.status_code == 200:
            print('Download succeeded! Writing file ...')
            open(path+EXCEL_FILE,'wb').write(response.content)


def get_positive_excel_from_tcd(auth_token: str, url: str, folders_: 1, path=''):
    '''
    This function downloads the excel from the TCD prod server.

    Args:

        - auth_token: Authentication token for the TCD server.
    '''

    folders = []
    if type(folders_) is not list:
        folders.append(folders_)
    else:
        folders = folders_.copy()
    
    if (path != '') and (not os.path.exists(path)):
        try:
            os.mkdir(path)
        except OSError:
            print ("Creation of the directory %s failed" % path)
            return
        else:
            print ("Successfully created the directory %s " % path)

    for folder_id in folders:

        POSITIVE_FILE = 'positivos-caso-'+str(folder_id)+'.csv'
        if os.path.exists(path+POSITIVE_FILE):

            print(f'WARNING: requested file { str(folder_id) } already exists, not downloading again.')
            continue
        
        print(f'Downoading positive data { str(folder_id) } from TCD site ...')

        querystring = {"folder":str(folder_id),"format":"csv"}

        payload = ""
        headers = {'Authorization': f'Token {auth_token}'}

        response = requests.request("GET", url, data=payload,
                                    headers=headers, params=querystring)

        if response.status_code == 200:
            print('Download succeeded! Writing file ...')
            open(path+POSITIVE_FILE,'wb').write(response.content)


def get_main_dataset(folders_, path=''):

    folders = []
    if type(folders_) is not list:
        folders.append(folders_)
    else:
        folders = folders_.copy()

    df = pd.DataFrame()

    for folder_id in folders:

        EXCEL_FILE = path+'caso-'+str(folder_id)+'.xlsx'
        df = df.append(pd.read_excel(EXCEL_FILE))

    return df


def get_positive_dataset(folders_, path=''):

    folders = []
    if type(folders_) is not list:
        folders.append(folders_)
    else:
        folders = folders_.copy()

    dfp = pd.DataFrame()

    for folder_id in folders:

        POSITIVE_FILE = path+'positivos-caso-'+str(folder_id)+'.csv'
        dfp = dfp.append(pd.read_csv(POSITIVE_FILE))

    return dfp


def read_parameters(name='special-scores.xlsx', path=''):

    EXCEL_FILE = path + name
    df = pd.read_excel(EXCEL_FILE)

    return df


'''
Funcao: monta_processos
Finalidade: Obter todos os processos de uma certidão positiva, montando uma string com eles
Parâmetros: 
              dfp -> dataset com as anotações positivas
             nome -> Nome do arquivo da certidão
Retorno: string com os números dos processos encontrados
'''
def monta_processos(dfp, nome):

    procs = dfp[dfp['Nome'] == nome].sort_values('Número do Processo')
    str_procs = ''
    for proc in procs.itertuples():
        str_procs += proc[3]
        
    return str_procs


'''
Funcao: validar_duplicidade
Finalidade: Verificar se existem linhas duplicadas em um dataset, considerando-se um conjunto de features,
            bem como os processos, em caso de certidão positiva
Parâmetros: 
               df -> dataset que se deseja verificar duplicidades
              dfp -> dataset com as anotações positivas
    cols_to_group -> lista com as colunas que devem ser verificadas
    col_to_report -> coluna que será informada no caso de erro
           folder -> Número da pasta de certidões que iremos processar
             save -> O resultado deve ser salvo em disco (True ou False)
             path -> Caminho para salvar o resultado
Retorno: dataframe com as linhas e mensagens de erro
'''
def validar_duplicidade (   df, 
                            dfp, 
                            cols_to_check=[], 
                            col_to_report='Url', 
                            folder='',
                            save=True, 
                            path=''):

    dupdf = df[df.duplicated(cols_to_check, keep=False)]

    str_procs = []
    result_idx = dupdf.columns.tolist().index('Resultado') + 1
    for lin in dupdf.itertuples():
        if lin[result_idx] == 'Positiva':
            str_procs.append(monta_processos(dfp, lin[1]))
        else:
            str_procs.append('')

    dupdf.insert(loc=dupdf.shape[1],column='Processos', value=str_procs ,allow_duplicates=True)
    cols_to_check2 = cols_to_check.copy()
    cols_to_check2.append('Processos')
    dup_w_process = dupdf.groupby(cols_to_check2, dropna=False)[col_to_report].apply(np.array).reset_index(name=col_to_report)

    urls = []
    erros = []
    grupos = []
    grupo = 1

    for line in dup_w_process.itertuples():
        if (len(line[len(cols_to_check2)+1])) > 1:
            for dupurl in line[len(cols_to_check2)+1]:
                grupos.append(grupo)
                urls.append(dupurl)
                erros.append('Possível certidão duplicada')
                
            grupo += 1

    erros_df = pd.DataFrame.from_dict({'Grupo': grupos, 'Url': urls, 'Mensagem': erros})
 
    if (save) and (erros_df.shape[0] > 0):
        timestr = time.strftime("%Y%m%d-%H%M%S")
        save_to = path + 'Certidões Duplicadas Folder [ ' + folder + ' ] - ' + timestr + '.xlsx'
        erros_df.to_excel(save_to, sheet_name='Erros')

    return erros_df


'''
Funcao: valida_data
Finalidade: Verifica se uma string possui uma data válida, de acordo com um formato
Parâmetros: 
             data -> String com a data
           format -> O formato em que a data deveria estar
    is_null_error -> Considera valores nulos errado ou não
Retorno: se a data for válida, retorna ela, caso contrário retorna ''
'''
def valida_data(data, format='%d/%m/%Y', is_null_error=False):

    try:
        date = datetime.strptime(data, format).date()
    except:
        date = ''

    return date


'''
Funcao: validar_datas
Finalidade: Verificar se colunas de datas de um dataframe estão corretas
Parâmetros: 
               df -> dataset que se deseja verificar as colunas
        cols_date -> lista com as colunas a serem verificadas
           format -> O formato em que as datas deveriam estar
    col_to_report -> coluna que será informada no caso de erro
    is_null_error -> Considera valores nulos errados ou não
           folder -> Número da pasta de certidões que iremos processar
             save -> O resultado deve ser salvo em disco (True ou False)
             path -> Caminho para salvar o resultado
Retorno: dataframe com as linhas e mensagens de erro
'''
def validar_datas ( df, 
                    cols_date=[], 
                    format='%d/%m/%Y', 
                    col_to_report='Url', 
                    is_null_error=False, 
                    folder='',
                    save=True, 
                    path=''):

    urls = []
    erros = []
    cols = cols_date.copy()
    cols.append(col_to_report)
    for line in df[cols].itertuples():
        for col in range(len(cols_date)):
            if pd.notna(line[col+1]):
                date = valida_data(line[col+1])
                if date == '':
                    urls.append(line[len(cols)])
                    erros.append('Data com problema: Coluna [ ' + cols_date[col] + ' ] Valor [ ' + line[col+1]+ ' ]')
            else:
                if is_null_error:
                    urls.append(line[len(cols)])
                    erros.append('Data com problema: Coluna [ ' + cols_date[col] + ' ]  Vazia')

    erros_df = pd.DataFrame.from_dict({'Url': urls, 'Mensagem': erros})

    if (save) and (erros_df.shape[0] > 0):
        timestr = time.strftime("%Y%m%d-%H%M%S")
        save_to = path + 'Certidões com Datas Erradas Folder [ ' + folder + ' ] - ' + timestr + '.xlsx'
        erros_df.to_excel(save_to, sheet_name='Erros')

    return erros_df


'''
Funcao: checar_validade
Finalidade: Checar a validade de uma certidão, a partir da data de sua emissão, em uma certa data
            Obs: A coluna de validade pode estar em dias, meses ou datas, ou ainda ser nula
Parâmetros: 
               df -> dataset que se deseja verificar as validades
  col_to_validate -> coluna com a validade obtida pela automação
      cols_issued -> coluna com a data de emissão
    col_to_report -> coluna que será informada no caso de erro
    is_null_error -> Flag que informa se as validades nulas serão consideradas erradas ou não
       limit_date -> Data contra a qual a validade será verificada
           folder -> Número da pasta de certidões que iremos processar
             save -> O resultado deve ser salvo em disco (True ou False)
             path -> Caminho para salvar o resultado
Retorno: dataframe com as linhas e mensagens de erro
'''
def checar_validade (   df, 
                        col_to_validate='Validade', 
                        col_issued='Emitido em', 
                        col_to_report='Url', 
                        is_null_error=False, 
                        limit_date='', 
                        folder='',
                        save=True, 
                        path=''):
    urls = []
    erros = []
    cols = list()
    cols.append(col_to_validate)
    cols.append(col_issued)
    cols.append(col_to_report)

    if limit_date == '':
        date_base = date.today()
    else:
        date_base =  valida_data(limit_date) 

    if date_base != '': # a data base é uma data válida, entao vamos checar

        for line in df[cols].itertuples():
            v = line[1]
            e = line[2]

            if (pd.isna(v)):
                v = ''

            if (pd.isna(e)):
                e = ''

            if (v == '') and (is_null_error):  # campo validade nulo, e isso é errado
                urls.append(line[3])
                erros.append('Validade com problema: Coluna [ ' + col_to_validate + ' ] nula')
                continue

            v = v.strip().lower() # campo validade
            e = e.strip() # campo emissao

            if (e == '') and (v == ''): # campo 'emitido em' e 'Validade' nulos
                urls.append(line[3])
                erros.append('Datas de Emissão e de Validade vazias')
                continue
            
            else: # Emissao ou Validade ou ambas contêm algo

                data_emi = valida_data(e) 

                if 'dias' in v: # validade em dias
                    try: # tenta pegar o número de dias
                        dias = int(v[0:3])
                    except:
                        urls.append(line[3])
                        erros.append('Validade com problema: Coluna [ ' + col_to_validate + ' ] Valor [ <vazio> ]')
                        continue
                elif 'meses' in v: # validade em meses
                    try:
                        meses = int(v[0:2]) # tenta pegar o número de meses
                    except:
                        urls.append(line[3])
                        erros.append('Validade com problema: Coluna [ ' + col_to_validate + ' ] Valor [ <vazio> ]')
                        continue

                    data_val = data_emi + relativedelta(months=meses)
                    dias = abs((data_val - data_emi).days)
                elif e: # validade em data
                    if v == '': # A Validade é vazia, mas isso não é erro
                        continue

                    data_val = valida_data(v) # Valida a data de validade
                    if data_val == '': # Não é uma data válida
                        urls.append(line[3])
                        erros.append('Validade com problema: Coluna [ ' + col_to_validate + ' ] Valor [ ' + v + ']')
                        continue

                    dias = abs((data_val - data_emi).days)

                if data_emi == '': # Sem data de emissão, mas com validade
                    valid_until = data_val
                else:
                    valid_until = data_emi + relativedelta(days=dias)
                    
                if valid_until < date_base:
                    urls.append(line[3])
                    erros.append('Validade Expirada: Válida até [ ' + valid_until.strftime("%d/%m/%Y") + ' ]')
                    continue

    erros_df = pd.DataFrame.from_dict({'Url': urls, 'Mensagem': erros}).sort_values('Mensagem')

    if (save) and (erros_df.shape[0] > 0):
        timestr = time.strftime("%Y%m%d-%H%M%S")
        save_to = path + 'Certidões com Validade Errada Folder [ ' + folder + ' ] - ' + timestr + '.xlsx'
        erros_df.to_excel(save_to, sheet_name='Erros')

    return erros_df


'''
Funcao: validar_cnpj_razao
Finalidade: Verificar se os nomes/razões sociais das certidões estão de acordo com os CPF/CNPJs
Parâmetros: 
               df -> dataset que se deseja verificar 
    col_reference -> Coluna que será usada como base
    cols_to_check -> Coluna que desejamos checar a consistência com a coluna de referência
    col_to_report -> coluna que será informada no caso de erro
        threshold -> percentual de similaridade a partir do qual uma razao social 
                     será considerada igual a outra
           folder -> Número da pasta de certidões que iremos processar
             save -> O resultado deve ser salvo em disco (True ou False)
             path -> Caminho para salvar o resultado

Situações inválidas:
1) Razão Social inconsistente: Razão social da certidão é bem diferente da
   esperada para aquele CPF/CNPJ
2) Nome/Razão Social sem CPF/CNPJ identificado: Certidão veio com Nome/Razão 
   social, mas sem CPF/CNPJ possível de ser identificado
3) Certidão sem CPF/CNPJ e sem Nome/Razão Social: Certidão sem CPF/CNPJ e sem 
   Nome/Razão social
4) Certidão sem CPF/CNPJ, porém identificável e atualizado para: A certidão 
   veio sem CPF/CNPJ, mas foi possível encontrar um com base no Nome/Razão social
5) Certidão sem CPF/CNPJ, mas Nome/Razão social [xxx] pode pertencer ao 
   CPF/CNPJ [xxx]:  A certidão veio sem CPF/CNPJ, e além disso sua razão social
   pode pertencer a mais de um CPF/CNPJ

Retorno: dataframe com as linhas e mensagem
'''
def validar_cnpj_razao( df, 
                        col_reference='Consultado (CPF/CNPJ)', 
                        col_to_check='Consultado (Nome)', 
                        col_to_report='Url', 
                        threshold=60, 
                        folder='',
                        save=True, 
                        path=''):

    urls = []
    urls_ref = []
    erros = []

    cols = df.columns.to_list()
    col_ref_idx = cols.index(col_reference) + 1
    col_che_idx = cols.index(col_to_check) + 1
    col_rep_idx = cols.index(col_to_report) + 1

    df_sem = df[df[col_reference].isna()]
    df_com = df[~df[col_reference].isna()]

    ###### Processa certidões COM CPF/CNPJ
    #    1) SEM RAZÃO SOCIAL -> ok
    #    2) COM RAZÃO SOCIAL -> verificar se aquele CPF/CNPJ possui mais de uma razão social diferente de nula
    #     2.1) Se não possuir -> ok
    #     2.2) Se possuir -> Erro

    for row in df_com.itertuples(): 
        if pd.isna(row[col_che_idx]) or (not row[col_che_idx]): # Sem Nome/Razão = OK
            continue
        else: # Se tiver Nome/Razão, verificar se tem outros Nomes/Razões diferentes para o mesmo CPF/CNPJ
            nomes = df.loc[lambda x: x[col_reference] == row[col_ref_idx]][col_to_check].dropna().unique()

            nomes_similares = process.extract(row[col_che_idx], nomes, limit=len(nomes))
            for nome in nomes_similares:
                if nome[1] < threshold: # Tem razão social, mas é bem diferente da atual
                    url_ref = df[df[col_to_check] == nome[0]][df[col_reference] == row[col_ref_idx]]['Url'].max()
                    urls.append(row[col_rep_idx])
                    urls_ref.append(url_ref)
                    erros.append('Mesmo CPF/CNPJ com Nomes/Razão Social distintos: [ '+row[col_che_idx]+' ] / [ '+nome[0]+' ]')
                    #erros.append('Razão Social inconsistente: Encontrado [ '+row[col_che_idx]+' ] Esperado [ '+nome[0]+' ]')
                    continue

    ###### Processa certidões SEM CPF/CNPJ
    #    1) SEM RAZÃO SOCIAL -> ERRO
    #    2) COM RAZAO SOCIAL -> Checar quais CPF/CNPJs, diferentes de nulo, com razão social igual
    #     2.1) Se não existir nenhuma outra certidão com a mesma razão social com CPF/CNPJ válido:
    #      2.1.1) Procurar CPF/CNPJs, diferentes de nulo, de razoes sociais parecidas
    #       2.1.1.1) Se não existir -> ERRO (Certidão sem CPF/CNPJ identificável)
    #       2.1.1.2) Se só existir apenas um, então associo o CPN/CNPJ à certidão
    #       2.1.1.3) Se existir mais de um CNPJ -> ERRO 
    #     2.2) Se só existir apenas um, então associo o CPN/CNPJ à certidão, porque ele tem a mesma
    #          razão social
    #     2.3) Se existir mais de um CNPJ para a mesma razão social -> ERRO 

    nomes_geral = df[col_to_check].dropna().unique()
    tam_nomes_geral = len(nomes_geral)

    for row in df_sem.itertuples(): # Processa certidões SEM CPF/CNPJ
        if pd.isna(row[col_che_idx]) or (not row[col_che_idx]): # Sem Nome/Razão = Erro
            urls.append(row[col_rep_idx])
            urls_ref.append('')
            erros.append('Certidão sem CPF/CNPJ e sem Nome/Razão Social')
           
        else: # Se tiver Nome/Razão, verificar se tem outros Nomes/Razões diferentes
            cnpjs = df.loc[lambda x: x[col_to_check] == row[col_che_idx]][col_reference].dropna().unique()
            if len(cnpjs) == 1:
                url_ref = df[df[col_to_check] == row[col_che_idx]][df[col_reference] == cnpjs[0]]['Url'].max()
                urls.append(row[col_rep_idx])
                urls_ref.append(url_ref)
                erros.append('Certidão sem CPF/CNPJ, porém identificável e atualizado para [ ' + cnpjs[0] + ' ]')
                df.loc[row[0], col_reference] = cnpjs[0]
            elif len(cnpjs) > 1:
                for cnpj in cnpjs:
                    url_ref = df[df[col_to_check] == row[col_che_idx]][df[col_reference] == cnpj]['Url'].max()
                    urls.append(row[col_rep_idx])
                    urls_ref.append(url_ref)
                    erros.append('Certidão sem CPF/CNPJ, mas Nome/Razão social 1 [ ' + row[col_che_idx] + ' ] pode pertencer ao CPF/CNPJ [ ' + cnpj + ' ]')
            else: # Não há outra certidão com a mesma razao social. Verificar por razões semelhantes
                similaridade = process.extract(row[col_che_idx], nomes_geral, limit=tam_nomes_geral)
                nomes_similares = []
                # Monta lista com as razões sociais semelhantes
                for nome in similaridade:
                    if nome[1] >= threshold:
                        nomes_similares.append(nome[0])

                cnpjs = df[df[col_to_check].isin(nomes_similares)][col_reference].dropna().unique()
                if len(cnpjs) == 0:
                    urls.append(row[col_rep_idx])
                    urls_ref.append('')
                    erros.append('Certidão com Nome/Razão Social, mas sem CPF/CNPJ identificável [ ' + row[col_che_idx] + ' ]')
                elif len(cnpjs) == 1:
                    url_ref = df[df[col_to_check] == row[col_che_idx]][df[col_reference] == cnpjs[0]]['Url'].max()
                    urls.append(row[col_rep_idx])
                    urls_ref.append('')
                    erros.append('Certidão sem CPF/CNPJ, porém identificável e atualizado para [ ' + cnpjs[0] + ' ]')
                    df.loc[row[0], col_reference] = cnpjs[0]
                elif len(cnpjs) > 1:
                    for cnpj in cnpjs:
                        url_ref = df[df[col_to_check].isin(nomes_similares)][df[col_reference] == cnpj]['Url'].max()
                        urls.append(row[col_rep_idx])
                        urls_ref.append(url_ref)
                        erros.append('Certidão sem CPF/CNPJ, mas Nome/Razão social 2 [ ' + row[col_che_idx] + ' ] pode pertencer ao CPF/CNPJ [ ' + cnpj + ' ]')

    erros_df = pd.DataFrame.from_dict({'Url': urls, 'Mensagem': erros, 'Url Referência': urls_ref}) 

    if (save) and (erros_df.shape[0] > 0):
        timestr = time.strftime("%Y%m%d-%H%M%S")
        save_to = path + 'Certidões com CPF ou CNPJ Inconsistente Folder [ ' + folder + ' ] - ' + timestr + '.xlsx'
        erros_df.to_excel(save_to, sheet_name='Erros')

    return df, erros_df


'''
Funcao auxiliar: totaliza_np
Finalidade: totalizar a quantidade de certidões de um certo tipo para um CNPJ (linha)
Parâmetros: 
              row -> linha de um dataframe
              skip -> número de colunas a pular (se houver colunas nãp acumuláveis
                       no início da linha)
              tipo -> P(Positiva) N(Negativa) PN(Positiva/Negativa)
           colunas -> lista de colunas do dataframe
Retorno: string formatada como link html
'''
def totaliza_np(row, skip, tipo, colunas):
    cols = []
    colunas = colunas[skip:] # Pular primeiras colunas sem dados a serem somados
    for idx, col in zip(range(len(colunas)), colunas):
        if (' '+tipo.center(2)) in col:
            cols.append(skip+idx)

    soma=0
    for i in cols:
        soma += row[i]

    return soma


'''
Funcao auxiliar: classif_result
Finalidade: criar o nome das colunas do mapa, que devem ser pequenos
Parâmetros: 
              row -> linha de um dataframe
Retorno: string formatada como link html
'''
def classif_result(row):
    if row['Resultado'] == 'Positiva':
        res = 'P '
    elif row['Resultado'] == 'Negativa':
        res = 'N '
    else:
        res = 'PN'
    return row['Classificação'][0:4] + res


'''
Funcao auxiliar: mask_map
Finalidade: criar uma máscara para que as células da matriz gerada pelo imshow 
            tenham as cores apropriadas
Parâmetros: 
             data -> dataframe com os dados da matriz
          label_x -> lista com os rótulos do eixo X
           totais -> indica se é para imprimir as colunas de totais ou não
Retorno: np.array com o mesmo tamanho do dataframe de dados, mas, em vez dos dados,
         os números correspondentes aos resultados das células

Resultado      Valor da máscara     Cor
----------     ----------------     ---------
 Sem result.      0                 Branca
 Positiva        10                 Vermelha
 Pos./Neg.       20                 Amarela
 Negativa        30                 Verde
 Totais          [40, 50, 60]       Tons de cinza
'''
def mask_map(data, label_x, totais=True, total_qt=3):
    if totais:
        inicio_total = len(label_x) - total_qt
    else:
        inicio_total = len(label_x)

    d = np.ones((data.shape[0], data.shape[1]))
    for i in range(data.shape[0]):
        for j in range(data.shape[1]):
            valor = data.iloc[i,j]

            if valor in ['Negativa', 'Positiva', 'Pos./Neg.', '']:
                if valor == 'Negativa':
                    d[i,j] = 30
                elif valor == 'Positiva': 
                    d[i,j] = 10
                else:
                    if valor == 'Pos./Neg.':
                        d[i,j] = 20
            else:
                if j < inicio_total:
                    if valor != 0:
                        if 'PN' in label_x[j]:
                            d[i,j] = 20
                        elif ' N ' in label_x[j]:
                            d[i,j] = 30
                        else:
                            d[i,j] = 10
                else: # totalizadores
                    d[i,j] = 40 + 10*(j - inicio_total)

    return d


'''
Funcao auxiliar: cria_colormap
Finalidade: criar um objeto ListedColorMap com as cores que serão utilizadas na 
            plotagem do mapa
Parâmetros: 
          NFAIXAS -> divisões do espectro de cores utilizadas
           totais -> indica se é para imprimir as colunas de totais ou não
Retorno: objeto com o mapa de cores
'''
def cria_colormap (NFAIXAS=280, totais=True):

    RED    = np.array([256/256,   0/256,   0/256, 1])
    WHITE  = np.array([256/256, 256/256, 256/256, 1])
    YELLOW = np.array([256/256, 256/256,   0/256, 1])
    GREEN  = np.array([  0/256, 256/256,   0/256, 1])
    GREY1  = np.array([220/256, 220/256, 220/256, 1])
    GREY2  = np.array([211/256, 211/256, 211/256, 1])
    GREY3  = np.array([192/256, 192/256, 192/256, 1])

    CORES = [WHITE, RED, YELLOW, GREEN, GREY1, GREY2, GREY3]

    # A partir de um colormap existente, modifica para imprimir somente 
    # quatro cores, conforme o Resultado da célula, ou sete cores se tiver totais
    map_cores = cm.get_cmap('RdYlGn', 256)

    # cria a representação RGB das cores desejadas
    #
    # Define as faixas em que as cores serão aplicadas, conforme os valores normalizados de cada 
    # célula
    # 280 é apenas para facilitar as faixas de cores para quando tem e não tem totais
    newcolors = map_cores(np.linspace(0, 1, NFAIXAS))
    ncores = 7 if totais else 4

    desloc = int(NFAIXAS/ncores)

    p1 = 0
    p2 = 0
    for c in range(ncores):
        p2 = (p1 + desloc) if (c < ncores) else (NFAIXAS)

        newcolors[p1:p2, ] = CORES[c]
        p1 = p2

    # Retorna novo colormap com a definição das faixas de cores
    return ListedColormap(newcolors)


'''
Funcao: gera_mapa_certidoes
Finalidade: criar um relatório(mapa) com os tipos de certidões e seus resultados, 
            agrupados por CPF/CNPJ
Parâmetros: 
               df -> dataframe com os dados já tratados
           totais -> indica se é para imprimir as colunas de totais ou não
           folder -> Número da pasta de certidões que iremos processar
             save -> O resultado deve ser salvo em disco (True ou False)
             path -> Caminho para salvar o resultado
Retorno: string formatada como link html
'''
def gera_mapa_certidoes(df, 
                        totais=True, 
                        folder='',
                        results='all',
                        save=True, 
                        path=''):

    RESULTADOS = {'Negativa': 'N', 'Positiva': 'P', 'Pos./Neg.': 'PN'}
    RESULTADOS_REV = {'N': 'Negativa', 'P': 'Positiva', 'PN': 'Pos./Neg.'}

    show_result = []
    if type(results) is list:
        for res in results:
            if res.upper() in RESULTADOS_REV:
                show_result.append(RESULTADOS_REV[res.upper()])
    else:
        if results.lower() == 'all':
            show_result = ['Negativa', 'Positiva', 'Pos./Neg.']
        else:
            if results.upper() in RESULTADOS_REV:
                show_result.append(RESULTADOS_REV[results.upper()])
            else:
                return

    CNPJ = 'Consultado (CPF/CNPJ)'
    CLASS = 'Classificação'
    RESULT = 'Resultado'
    CLASS_RES = 'classif_result'

    data = df[[CNPJ, CLASS, RESULT]][df[RESULT].isin(show_result)]
    data[CLASS_RES] = data.apply(classif_result, axis=1)
    data = data.set_index(CNPJ)
    data = pd.get_dummies(data=data, columns=[CLASS_RES], prefix='', prefix_sep='').groupby(CNPJ).sum()

    colunas = data.columns.tolist()
    
    # Insere colunas totalizadoras, se for o caso
    if totais:
        for res in show_result: #RESULTADOS:
            data['Tot '+RESULTADOS[res]] = data.apply(totaliza_np, args=[0, RESULTADOS[res], colunas], axis=1)

    label_y = np.array(data.index.tolist())
    label_x = data.columns.values.tolist()

    inicio_total = (len(label_x) - len(show_result)) if totais else (len(label_x))

    mask = mask_map(data, label_x, totais, len(show_result))

    # Obtem o novo colormap
    newcmp = cria_colormap(totais=totais)

    fig, ax = plt.subplots(figsize = (22, 22), facecolor='w')

    vmax = 60 if totais else 30
    ax.imshow(mask, cmap=newcmp, norm=colors.Normalize(vmin=0, vmax=vmax)) 

    # Exibe todos os ticks dos eixos x e y
    ax.xaxis.set_major_locator(ticker.MultipleLocator(1)) 
    ax.yaxis.set_major_locator(ticker.MultipleLocator(1)) 

    # Exibe os rótulos nos eixos x e y
    plt.xticks(range(len(label_x)), label_x, rotation='vertical') 
    plt.yticks(range(len(label_y)), label_y) 

    # rótulos na parte superior e inferior para melhor legibilidade
    ax.tick_params(axis="x", bottom=True, top=True, labelbottom=True, labeltop=True) 

    plt.title('Mapa de Certidões Folder [ '+str(folder)+' ]')

    # Coloca o texto nas células, que é a quantidade de cada tipo de certidão x cnpj
    for i in range(data.shape[0]):
        for j in range(data.shape[1]):
            cell = data.iloc[i, j]
            if (j < inicio_total): # preenche as células de valores
                if cell > 0: # somente se houver uma ou mais certidões
                    ax.text(j,i,str(cell), va='center', ha='center', fontsize=14)
            else: # preenche as células totalizadoras
                ax.text(j,i,str(cell), va='center', ha='center', fontsize=14)

    fig.show()

    if save:
        timestr = time.strftime("%Y%m%d-%H%M%S")
        save_to = path + 'Mapa de Certidões Folder [ ' + folder + ' ] - ' + timestr + '.jpg'
        plt.savefig(save_to, dpi=150)

    return data


def gera_sheet_certidoes(df, 
                        folder='',
                        save=True, 
                        path=''):


    df2 = df[['Consultado (CPF/CNPJ)', 'Classificação', 'Resultado', 'Emitido em']].copy()
    df2['class_short'] = df2['Classificação'].apply(lambda x: x[0:4])
    df2 = df2[['Consultado (CPF/CNPJ)', 'Resultado', 'class_short']].groupby(by=['Consultado (CPF/CNPJ)', 'class_short']).max('Emitido em').reset_index()
    df2 = df2.pivot(index='Consultado (CPF/CNPJ)', columns='class_short', values='Resultado').reset_index().set_index('Consultado (CPF/CNPJ)')
    df2.fillna('', inplace=True)

    label_y = np.array(df2.index.tolist())
    label_x = df2.columns.values.tolist()

    mask = mask_map(df2, label_x, totais=False, total_qt=3)

    newcmp = cria_colormap(totais=False)

    fig, ax = plt.subplots(figsize = (22, 22), facecolor='w')

    ax.imshow(mask, cmap=newcmp, norm=colors.Normalize(vmin=0, vmax=30)) 

    # Exibe todos os ticks dos eixos x e y
    ax.xaxis.set_major_locator(ticker.MultipleLocator(1)) 
    ax.yaxis.set_major_locator(ticker.MultipleLocator(1)) 

    # Exibe os rótulos nos eixos x e y
    plt.xticks(range(len(label_x)), label_x, rotation='vertical') 
    plt.yticks(range(len(label_y)), label_y) 

    # rótulos na parte superior e inferior para melhor legibilidade
    ax.tick_params(axis="x", bottom=True, top=True, labelbottom=True, labeltop=True) 

    plt.title('Mapa de Certidões Folder [ '+str(folder)+' ]')

    fig.show()

    if save:
        timestr = time.strftime("%Y%m%d-%H%M%S")
        save_to = path + 'Sheet de Certidões Folder [ ' + folder + ' ] - ' + timestr + '.jpg'
        plt.savefig(save_to, dpi=150)

    return df2


def get_supplier_score(row, exception_rules, columns):
    total = 0.0
    for pos, col in zip(range(len(columns)), columns):
        col = col.strip()
        if row[pos] == 'Positiva':
            if col in exception_rules:
                total += exception_rules[col]
            else:
                total += 1
        elif row[pos] == 'Pos./Neg.':
            total += 0.5
    
    return total

def suppliers_score(df, 
                    suppliers,
                    special_scores,
                    folder='',
                    save=True, 
                    path=''):

    df2 = df[['Consultado (CPF/CNPJ)', 'Classificação', 'Resultado', 'Emitido em']].copy()
    df2['class_short'] = df2['Classificação'].apply(lambda x: x[0:4])
    df2 = df2[['Consultado (CPF/CNPJ)', 'Resultado', 'class_short']].groupby(by=['Consultado (CPF/CNPJ)', 'class_short']).max('Emitido em').reset_index()
    df2 = df2.pivot(index='Consultado (CPF/CNPJ)', columns='class_short', values='Resultado').reset_index().set_index('Consultado (CPF/CNPJ)')
    df2.fillna('', inplace=True)

    df2['score'] = 0
    columns = df2.columns.values.tolist()
    columns.remove('score')

    for row in df2.itertuples():
        sup_param = suppliers.loc[suppliers.CNPJ_CPF == row[0], ['Classificação', 'Terceiro']]
        curve_ABC = ord(sup_param.iloc[0,0])-64
        third_local = sup_param.iloc[0,1]
        
        sp_sc_dict = {}
        for row_special in special_scores.itertuples(index=False):
            sp_sc_dict[str(row_special[0])] = row_special[curve_ABC + (3*third_local)]

        score = get_supplier_score(row[1:], sp_sc_dict, columns)
        df2.loc[row[0], 'score'] = score

    max_score = df2.score.max()
    score_good = max_score*0.3
    score_bad = max_score*0.7

    labels_x = df2.index.values

    size_x = int(len(labels_x)/3.0)+1
    size_y = int(max_score/3.0)+1

    fig, ax = plt.subplots(figsize = (size_y, size_x), facecolor='w')

    colors = df2.score.apply(lambda x: 'red' if x > score_bad else 'green' if x <= score_good else 'yellow').tolist()

    ax.tick_params(axis="x", bottom=True, top=True, labelbottom=True, labeltop=True) 

    plt.title('Pontuação de Fornecedores [ '+str(folder)+' ]')
    plt.scatter(df2.score, labels_x, c=colors, s=200)

    plt.grid(b=True, which='major', color='#666666', linestyle='-')
    plt.minorticks_on()
    plt.grid(b=True, which='minor', color='#999999', linestyle='--', alpha=0.2, axis='x')

    fig.show()

    if save:
        timestr = time.strftime("%Y%m%d-%H%M%S")
        save_to = path + 'Pontuação de Fornecedores [ ' + folder + ' ] - ' + timestr + '.jpg'
        plt.savefig(save_to, dpi=150)

    return df2


'''
Funcao auxiliar: make_clickable
Finalidade: exibir uma URL clicável
Parâmetros: 
              url -> campo string que contém uma URL
Retorno: string formatada como link html
'''
def make_clickable(url):
  return f'<a href="{url}">{url}</a>'
