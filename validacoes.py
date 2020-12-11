import os

import pandas as pd
import numpy as np
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

from fuzzywuzzy import fuzz
from fuzzywuzzy import process

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
Retorno: dataframe com as linhas e mensagens de erro
'''
def validar_duplicidade (df, dfp, cols_to_check=[], col_to_report='Url'):
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
    grupo = 0

    for line in dup_w_process.itertuples():
        if (len(line[len(cols_to_check2)+1])) > 1:
            for dupurl in line[len(cols_to_check2)+1]:
                grupos.append(grupo)
                urls.append(dupurl)
                erros.append('Possível certidão duplicada')
                
            grupo += 1

    return pd.DataFrame.from_dict({'Grupo': grupos, 'Url': urls, 'Mensagem': erros})


'''
Funcao: valida_data
Finalidade: Verifica se uma string possui uma data válida, de acordo com um formato
Parâmetros: 
             data -> String com a data
           format -> O formato em que a data deveria estar
Retorno: se a data for válida, retorna ela, caso contrário retorna ''
'''
def valida_data(data, format='%d/%m/%Y'):
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
Retorno: dataframe com as linhas e mensagens de erro
'''
def validar_datas (df, cols_date=[], format='%d/%m/%Y', col_to_report='Url'):
    urls = []
    erros = []
    cols = cols_date.copy()
    cols.append(col_to_report)
    for line in df[cols].itertuples():
        for col in range(len(cols_date)):
            date = valida_data(line[col+1])
            if date == '':
                urls.append(line[len(cols)])
                if pd.notna(line[col+1]):
                    erros.append('Data com problema: Coluna [ ' + cols_date[col] + ' ] Valor [ ' + line[col+1]+ ' ]')
                else:
                    erros.append('Data com problema: Coluna [ ' + cols_date[col] + ' ] Valor [ Nan ]')

    return pd.DataFrame.from_dict({'Urls': urls, 'Mensagem': erros})


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
Retorno: dataframe com as linhas e mensagens de erro
'''
def checar_validade (df, col_to_validate='Validade', col_issued='Emitido em', col_to_report='Url', is_null_error=True, limit_date=''):
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

    if date_base != '':

        for line in df[cols].itertuples():
            if pd.isna(line[1]):  # campo validade nulo
                if is_null_error:
                    urls.append(line[3])
                    erros.append('Validade com problema: Coluna [ ' + col_to_validate + ' ] Valor [ Nan ]')
                    continue
            elif not line[1].strip(): # campo validade vazio
                if is_null_error:
                    urls.append(line[3])
                    erros.append('Validade com problema: Coluna [ ' + col_to_validate + ' ] Valor [ <vazio> ]')
                    continue
            else:
                v = line[1].lower().strip() # campo validade

                if (pd.isna(line[2])): # campo 'emitido em' nulo
                    urls.append(line[3])
                    erros.append('Data com problema: Coluna [ ' + col_issued + ' ] Valor [ Nan ]')
                    continue
                else:
                    e = line[2].lower().strip()

                    if e != '': # campo 'emitido em' não vazio

                        data_emi = valida_data(e) 
                        if data_emi == '': # data de emissao com problema
                            urls.append(line[3])
                            erros.append('Data com problema: Coluna [ ' + col_issued + ' ] Valor [ ' + e +' ]')
                            continue

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
                            data_val = valida_data(v)
                            if data_val == '':
                                urls.append(line[3])
                                erros.append('Validade com problema: Coluna [ ' + col_to_validate + ' ] Valor [ <vazio> ]')
                                continue

                            dias = abs((data_val - data_emi).days)

                    else: # campo 'emitido em' vazio
                        urls.append(line[3])
                        erros.append('Validade com problema: Coluna [ ' + col_issued + ' ] Valor [ <vazio> ]')
                        continue

                    valid_until = data_emi + relativedelta(days=dias)
                        
                    if valid_until < date_base:
                        urls.append(line[3])
                        erros.append('Validade Expirada: Válida até [ ' + valid_until.strftime("%d/%m/%Y") + ' ]')
                        continue

    return pd.DataFrame.from_dict({'Urls': urls, 'Mensagem': erros})


'''
Funcao: validar_cnpj_razao
Finalidade: Verificar se os nomes/razões sociais das certidões estão de acordo com os CPF/CNPJs
Parâmetros: 
               df -> dataset que se deseja verificar 
    col_reference -> Coluna que será usada como base
    cols_to_check -> Coluna que desejamos checar a consistência com a coluna de referência
    col_to_report -> coluna que será informada no caso de erro
Retorno: dataframe com as linhas e mensagem
'''
def validar_cnpj_razao(df, col_reference='Consultado (CPF/CNPJ)', col_to_check='Consultado (Nome)', col_to_report='Url', threshold=60):

    urls = []
    erros = []

    dfs = df.sort_values(by=[col_reference, col_to_check])

    cols = df.columns.to_list()
    col_ref_idx = cols.index(col_reference) + 1
    col_che_idx = cols.index(col_to_check) + 1
    col_rep_idx = cols.index(col_to_report) + 1

    cnpj_atual = ''
    razao_atual = ''
    primeiro = True
    for lin in dfs.itertuples():
        nome_raz = lin[col_che_idx]

        if (lin[col_ref_idx] != '') and (not pd.isna(lin[col_ref_idx])): # Somente se o cnpj contiver alguma coisa

            if primeiro:
                if lin[col_ref_idx].strip() != cnpj_atual:
                    cnpj_atual = lin[col_ref_idx].strip()
                    razao_atual = nome_raz
                    primeiro = False
                    continue
                else:
                    primeiro = False
            else:
                if lin[col_ref_idx].strip() != cnpj_atual:
                    cnpj_atual = lin[col_ref_idx].strip()
                    razao_atual = nome_raz
                    primeiro = True
                    continue

            if (pd.isna(nome_raz)): # Tem CPF/CNPJ mas não tem Nome/Razao = OK 
                continue

            perc_similar = fuzz.token_sort_ratio(nome_raz, razao_atual)
            if (perc_similar < threshold): # A razao é bastante diferente
                urls.append(lin[col_rep_idx])
                erros.append('Razão Social inconsistente: Encontrado [ '+lin[col_che_idx]+' ] Esperado [ '+razao_atual+' ]')
        else: # CPF/CNPJ nulo ou vazio
            if (pd.isna(nome_raz)): # Não possui razão social/nome também = ERRO
                urls.append(lin[col_rep_idx])
                erros.append('Certidão sem CPF/CNPJ e sem Nome/Razão Social')
            else: # Não tem CPF/CNPJ mas possui razao social/nome
                num_cnpjs = df.loc[lambda x: x[col_to_check] == nome_raz][col_reference].unique()
                if (len(num_cnpjs) == 1): 
                    if (pd.isna(num_cnpjs)): # Com Nome/Razão mas o único CPF/CNPJ é nulo = ERRO
                        urls.append(lin[col_rep_idx])
                        erros.append('Certidão com Nome/Razão Social, mas sem CPF/CNPJ identificável [ ' + nome_raz + ' ]')
                elif (len(num_cnpjs) == 2): # Um dos cnpjs é nulo. Devemos checar o outro e atualizar o df
                    cnpj_tmp = num_cnpjs[1] if (pd.isna(num_cnpjs[0])) else num_cnpjs[0]
                    # atualiza o CPF/CNPJ quando há apenas um CPF/CNPJ, com a mesma razao social, que não é nulo
                    for index, row in df.loc[lambda x: x[col_to_check] == nome_raz].iterrows():
                        urls.append(row[col_rep_idx-1])
                        erros.append('Certidão sem CPF/CNPJ, porém identificável e atualizado para [ ' + cnpj_tmp+ ' ]')
                        df.loc[index, col_reference] = cnpj_tmp
                else:  # Mais que dois Nomes/CNPJs é um problema = ERRO. Mostra os CPFs/CNPJs possíveis
                    for cnpj in num_cnpjs:
                        if (not pd.isna(cnpj)):
                            urls.append(lin[col_rep_idx])
                            erros.append('Certidão sem CPF/CNPJ, mas Nome/Razão social [ ' + nome_raz + ' ] pode pertencer ao CPF/CNPJ [ ' + cnpj + ' ]')

    return df, pd.DataFrame.from_dict({'Urls': urls, 'Mensagem': erros})


def totaliza_np(row, skip, t, size):
    soma=0
    for i in range(skip+t-1, size, 3): soma += row[i]

    return soma


def gera_mapa_certidoes(df):
    cont = 0
    tipos = {}
    for n in (df['Classificação'].unique()):
        tipos[n] = cont
        cont += 1

    num_linhas = len(df[~df['Consultado (CPF/CNPJ)'].isna()]['Consultado (CPF/CNPJ)'].unique())

    resultados = ['Negativa', 'Positiva', 'Pos./Neg.']

    dfg = df[~df['Consultado (CPF/CNPJ)'].isna()].groupby(['Consultado (CPF/CNPJ)', 'Classificação', 'Resultado']).count()['Url']
    dfg_i = dfg.index

    cont_cnpj = -1
    cnpj_atual = ''
    cont_lin = 0
    cert_dict = {}
    cert_dict['cnpj'] = [] 

    for tipo in tipos:
        for res in resultados:
            cert_dict[tipo[0:4]+' '+res] = np.zeros(num_linhas,  dtype=int)

    for lin in dfg:
        if (cnpj_atual != dfg_i[cont_lin][0]):
            cnpj_atual = dfg_i[cont_lin][0]
            cont_cnpj += 1
            cert_dict['cnpj'].append(cnpj_atual) 

        tipo_cert = dfg_i[cont_lin][1]
        resu_cert = dfg_i[cont_lin][2]

        cert_dict[tipo_cert[0:4]+' '+resu_cert][cont_cnpj] = lin

        cont_lin += 1

    mapdf = pd.DataFrame.from_dict(cert_dict) 
    tam_mapdf = mapdf.shape[1]
    
    # Insere colunas totalizadoras
    tipo = 1
    for res in resultados:
        mapdf['Total'+' '+res] = mapdf.apply(totaliza_np, args=[1, tipo, tam_mapdf], axis=1)
        tipo += 1

    return mapdf
