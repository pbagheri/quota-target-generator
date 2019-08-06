# -*- coding: utf-8 -*-
"""
Created on Fri Jun 29 09:57:04 2018

@author: payam.bagheri
"""

import pandas as pd
from os import path
import itertools

 

dir_path = path.dirname(path.abspath(path.abspath(__file__)))
filename = str(dir_path) + '\\0_input_data\\w_vars.xlsx'
print(filename)

wdat = pd.read_excel(filename)

def name_sel(lis, sel_n):
    combs = []
    for subset in itertools.combinations(range(len(lis)),sel_n):
        combs.append([lis[x] for x in subset])
    return combs
       


def cross(df1, df2, **kwargs):
    """
    Make a cross join (cartesian product) between two dataframes by using a constant temporary key.
    Also sets a MultiIndex which is the cartesian product of the indices of the input dataframes.
    See: https://github.com/pydata/pandas/issues/5401
    :param df1 dataframe 1
    :param df1 dataframe 2
    :param kwargs keyword arguments that will be passed to pd.merge()
    :return cross join of df1 and df2
    """
    df1['_tmpkey'] = 1
    df2['_tmpkey'] = 1

    res = pd.merge(df1, df2, on='_tmpkey', **kwargs).drop('_tmpkey', axis=1)
    res.index = pd.MultiIndex.from_product((df1.index, df2.index))

    df1.drop('_tmpkey', axis=1, inplace=True)
    df2.drop('_tmpkey', axis=1, inplace=True)
    return res


def str_cross(df1,df2):
    tem = []
    for i in list(df1.dropna()):
        for j in list(df2.dropna()):
            tem.append(i + ',' + j)
            tem_s = pd.Series(tem)
    return tem_s

# Drops an element from a list
def list_elem_drop(ls,k):   
    d = 0
    nn = ls[0]
    while nn != k:
        d += 1
        nn = ls[d]
    del ls[d]
    
    
tar_col_names = [x for x in wdat.columns if (wdat[x].dtype == 'float64' or wdat[x].dtype == 'int64')]
nam_col_names = [x for x in wdat.columns if wdat[x].dtype == 'O']
cross_coefs = pd.DataFrame()

dub_names = name_sel(nam_col_names,2)
dub_tars = name_sel(tar_col_names,2)

for (i,j) in zip(dub_tars, dub_names):
    i_tar, j_tar = i[0], i[1]
    i_nam, j_nam = j[0], j[1]
    a = pd.DataFrame(wdat[i_tar])
    b = pd.DataFrame(wdat[j_tar])
    ij = cross(a,b)
    itj = ij[i_tar]*ij[j_tar]
    t = pd.Series(list(itj)).dropna()
    t.reset_index(inplace=True,drop=True)
    t_nam = str_cross(wdat[i_nam],wdat[j_nam])
    t_nam.reset_index(inplace=True,drop=True)
    cross_coefs = pd.concat([cross_coefs,t_nam.rename(i_nam+','+j_nam)], axis=1)
    cross_coefs = pd.concat([cross_coefs,t.rename(i_tar+','+j_tar)], axis=1)  


tar_col_names = [x for x in wdat.columns if (wdat[x].dtype == 'float64' or wdat[x].dtype == 'int64')]
nam_col_names = [x for x in wdat.columns if wdat[x].dtype == 'O']

dub_names = name_sel(nam_col_names,3)
dub_tars = name_sel(tar_col_names,3)

for (i,j) in zip(dub_tars, dub_names):
    i_tar, j_tar, k_tar = i[0], i[1], i[2]
    i_nam, j_nam, k_nam = j[0], j[1], j[2]
    a = pd.DataFrame(wdat[i_tar])
    b = pd.DataFrame(wdat[j_tar])
    c = pd.DataFrame(wdat[k_tar])
    ij = cross(a,b)
    ij = ij.reset_index()
    ijk = cross(ij,c)
    itjtk = ijk[i_tar]*ijk[j_tar]*ijk[k_tar]
    t = pd.Series(list(itjtk))
    t = pd.Series(list(itjtk)).dropna()
    t.reset_index(inplace=True,drop=True)
    t_nam = str_cross(wdat[i_nam],wdat[j_nam])
    t_nam = str_cross(t_nam,wdat[k_nam])
    t_nam.reset_index(inplace=True,drop=True)
    cross_coefs = pd.concat([cross_coefs,t_nam.rename(i_nam+','+j_nam+','+k_nam)], axis=1)
    cross_coefs = pd.concat([cross_coefs,t.rename(i_tar+','+j_tar+','+k_tar)], axis=1)

cross_coefs.to_excel(dir_path + '\\0_output\\cross_var_coefs.xlsx', index=False, header=True)  
