#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author: similraface
# @contact: similarface@gmail.com
# @software: PyCharm
# @file: test_mul.py
# @time: 2022/6/20 11:25 上午
# @desc:
import pandas as pd
from snpmerge.mul import merge_snp_paths


def test_merge_snp_paths():
    paths = ['./data/f1.csv', './data/f2.csv', './data/f3.csv']
    result = merge_snp_paths(paths=paths, output=None, sep=',', suffix=".csv", header_index_str='id,chrom,position,ref')

    df1 = pd.read_csv(paths[0])
    df2 = pd.read_csv(paths[1])

    merge_df = df1.merge(df2, on=['id', 'chrom', 'position', 'ref'])
    merge_df_by_msp = pd.read_csv(result)
    assert merge_df.shape[0] == merge_df_by_msp.shape[0]
    assert abs(merge_df.shape[1] - merge_df_by_msp.shape[1]) <= 1
    assert merge_df['id'].to_list() == merge_df_by_msp['id'].to_list()
