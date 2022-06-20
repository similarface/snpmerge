#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author: similraface
# @contact: similarface@gmail.com
# @software: PyCharm
# @file: merged.py
# @time: 2022/6/20 9:23 上午
# @desc:
import os
import time
import tempfile
import tqdm
import shutil
from concurrent.futures.process import ProcessPoolExecutor
from itertools import zip_longest
from concurrent import futures
from loguru import logger
import pandas as pd


def line_merge_return(index, left_data, right_data, sep=','):
    """
    行拼接返回
    """
    return sep.join([sep.join(index), left_data, right_data])


def line_sep_split(line, sep, sep_pos_ct):
    """
    行切割
    :param line: 一行的数据
    :param sep: 切割符号
    :param sep_pos_ct: 索引占有的分隔符号的个数
    :return:
        :l_index   索引
        :data_data 数据
    """
    *_l_index, data_data = line.split(sep, sep_pos_ct)
    l_index = tuple(_l_index)
    return l_index, data_data.rstrip("\n")


def _line_merge_data(merge_data, left_index, left_data, right_index, right_data, sep, how, null_left_data, null_right_data):
    """
    单次左、右 行数据的merge结果
    :param merge_data:
    :param left_index:
    :param left_data:
    :param right_index:
    :param right_data:
    :param sep:
    :param how:
    :param null_left_data:
    :param null_right_data:
    :return:
    """
    if left_index == right_index:
        merge_data.append(line_merge_return(left_index, left_data, right_data, sep=sep))
    elif left_index > right_index and how in ['right', 'outer']:
        merge_data.append(line_merge_return(right_index, null_left_data, right_data, sep=sep))
    elif left_index < right_index and how in ['left', 'outer']:
        merge_data.append(line_merge_return(left_index, left_data, null_right_data, sep=sep))


def _right_next(merge_data, right_oper, left_index, left_data, sep, sep_pos_ct, null_left_data, null_right_data, how):
    """
    # 左侧索引大，遍历右侧
    :param merge_data:
    :param right_oper:
    :param left_index:
    :param left_data:
    :param sep:
    :param sep_pos_ct:
    :param null_left_data:
    :param null_right_data:
    :param how:
    :return:
    """
    while True:
        _curr_right_offset = right_oper.tell()
        right_index, right_data = line_sep_split(right_oper.readline(), sep=sep, sep_pos_ct=sep_pos_ct)
        if left_index == right_index:
            _line_merge_data(merge_data=merge_data, left_index=left_index, left_data=left_data,
                             right_index=right_index, right_data=right_data,
                             sep=sep, how=how,
                             null_left_data=null_left_data, null_right_data=null_right_data)
            return merge_data, False
        elif left_index > right_index:
            _line_merge_data(merge_data=merge_data, left_index=left_index, left_data=left_data,
                             right_index=right_index, right_data=right_data,
                             sep=sep, how=how,
                             null_left_data=null_left_data, null_right_data=null_right_data)
        else:
            # 未找到匹配的
            # 指针退回
            _line_merge_data(merge_data=merge_data, left_index=left_index, left_data=left_data,
                             right_index=right_index, right_data=right_data,
                             sep=sep, how=how,
                             null_left_data=null_left_data, null_right_data=null_right_data)
            right_oper.seek(_curr_right_offset)
            return merge_data, False


def _left_next(merge_data, left_oper, right_index, right_data, sep, sep_pos_ct, null_left_data, null_right_data, how):
    """
     # 左侧索引小，遍历左侧
    :param merge_data:
    :param left_oper:
    :param right_index:
    :param right_data:
    :param sep:
    :param sep_pos_ct:
    :param null_left_data:
    :param null_right_data:
    :param how:
    :return:
    """
    while True:
        _curr_left_offset = left_oper.tell()
        left_index, left_data = line_sep_split(left_oper.readline(), sep=sep, sep_pos_ct=sep_pos_ct)
        if left_index == right_index:
            _line_merge_data(merge_data=merge_data, left_index=left_index, left_data=left_data,
                             right_index=right_index, right_data=right_data,
                             sep=sep, how=how,
                             null_left_data=null_left_data, null_right_data=null_right_data)
            return merge_data, False
        # 未找到匹配的
        elif left_index < right_index:
            _line_merge_data(merge_data=merge_data, left_index=left_index, left_data=left_data,
                             right_index=right_index, right_data=right_data,
                             sep=sep, how=how,
                             null_left_data=null_left_data, null_right_data=null_right_data)
        else:
            _line_merge_data(merge_data=merge_data, left_index=left_index, left_data=left_data,
                             right_index=right_index, right_data=right_data,
                             sep=sep, how=how,
                             null_left_data=null_left_data, null_right_data=null_right_data)
            # 未找到匹配的指针退回
            left_oper.seek(_curr_left_offset)
            return merge_data, False


def _right_next_when_left_done(merge_data, right_oper, r_line, sep, sep_pos_ct, how, null_left_data):
    """
    左测没有数据 右侧有数据 遍历右侧数据
    :return:
    """
    right_index, right_data = line_sep_split(line=r_line, sep=sep, sep_pos_ct=sep_pos_ct)
    merge_data.append(line_merge_return(right_index, null_left_data, right_data, sep=sep))
    if how in ['outer', 'right']:
        while True:
            _r_line = right_oper.readline()
            if _r_line:
                r_index, r_data = line_sep_split(line=_r_line, sep=sep, sep_pos_ct=sep_pos_ct)
                merge_data.append(line_merge_return(r_index, null_left_data, r_data, sep=sep))
            else:
                return merge_data, True
    raise OSError()


def _left_next_when_right_done(merge_data, left_oper, l_line, sep, sep_pos_ct, how, null_right_data):
    left_index, left_data = line_sep_split(line=l_line, sep=sep, sep_pos_ct=sep_pos_ct)
    merge_data.append(line_merge_return(left_index, left_data, null_right_data, sep=sep))
    if how in ['outer', 'left']:
        while True:
            _l_line = left_oper.readline()
            if _l_line:
                _l_index, _l_data = line_sep_split(line=_l_line, sep=sep, sep_pos_ct=sep_pos_ct)
                merge_data.append(line_merge_return(_l_index, _l_data, null_right_data, sep=sep))
            else:
                return merge_data, True
    raise OSError()


def compare_next(left_oper, right_oper, sep=',', sep_pos_ct=4, how='inner',
                 right_sample_ct=0,
                 left_sample_ct=0, ):
    """
    单次左右的比对，直到左右相等【或者到文件尾部】 认为完成一次迭代
    :param left_oper: 左文件句柄
    :param right_oper:右文件句柄
    :param sep: 行切割符号
    :param sep_pos_ct: 索引占有的分隔符号的个数
    :param how: 如何join， ['inner','left','right','outer']
    :param right_sample_ct: 右样本数目
    :param left_sample_ct: 左样本数目
    :return:
        :merge_data: 返回的新行数据
        :flag: 是否完成遍历
    """
    null_right_data = sep.join([''] * right_sample_ct) + "\n"
    null_left_data = sep.join([''] * left_sample_ct) + "\n"
    l_line = left_oper.readline()
    r_line = right_oper.readline()
    # 左右测都有数据
    merge_data = []
    if l_line and r_line:
        left_index, left_data = line_sep_split(line=l_line, sep=sep, sep_pos_ct=sep_pos_ct)
        right_index, right_data = line_sep_split(line=r_line, sep=sep, sep_pos_ct=sep_pos_ct)
        # 匹配成功
        if left_index == right_index:
            # 数据直接返回
            _line_merge_data(merge_data=merge_data, left_index=left_index, left_data=left_data,
                             right_index=right_index, right_data=right_data,
                             sep=sep, how=how,
                             null_left_data=null_left_data, null_right_data=null_right_data)
            return merge_data, False
        # 左侧索引大，遍历右侧
        elif left_index > right_index:
            # how_return(left_index, left_data, right_data)
            # 右边需要向下移动
            _line_merge_data(merge_data=merge_data, left_index=left_index, left_data=left_data,
                             right_index=right_index, right_data=right_data,
                             sep=sep, how=how,
                             null_left_data=null_left_data, null_right_data=null_right_data)
            return _right_next(merge_data, right_oper, left_index, left_data, sep, sep_pos_ct, null_left_data, null_right_data, how)
        # 左侧索引小，遍历左侧
        else:
            _line_merge_data(merge_data=merge_data, left_index=left_index, left_data=left_data,
                             right_index=right_index, right_data=right_data,
                             sep=sep, how=how,
                             null_left_data=null_left_data, null_right_data=null_right_data)
            # 左边需要向下移动
            return _left_next(merge_data, left_oper, right_index, right_data, sep, sep_pos_ct, null_left_data, null_right_data, how)
    # 左测没有数据 右侧有数据
    if not l_line and r_line:
        return _right_next_when_left_done(merge_data, right_oper, r_line, sep, sep_pos_ct, how, null_left_data)
    # 右边测没有数据 左侧有数据
    if not r_line and l_line:
        return _left_next_when_right_done(merge_data, left_oper, l_line, sep, sep_pos_ct, how, null_right_data)
    # 左右边测都没有数据
    if not r_line and not l_line:
        raise OSError()


def merge_file(left_path, right_path, output_path, header_index_str, sep=',', how='inner', input_mode='r', output_mode='w'):
    assert how in ['inner', 'left', 'right', 'outer'], "how must in ['inner', 'left', 'right', 'outer']"
    header_index_str = header_index_str.rstrip(sep)
    sep_pos_ct = len(header_index_str.split(sep))
    with open(left_path, input_mode) as left_open, open(right_path, input_mode) as right_open, open(output_path, output_mode) as woper:
        try:
            header1: str = left_open.readline()
            header2: str = right_open.readline()
        except Exception:
            print(f"left {left_path} {right_path}")
        if not header1.startswith(header_index_str) and not header2.startswith(header_index_str):
            raise RuntimeError("格式不支持")
        l_index, append_header1 = line_sep_split(line=header1, sep=sep, sep_pos_ct=sep_pos_ct)
        h_index, append_header2 = line_sep_split(line=header2, sep=sep, sep_pos_ct=sep_pos_ct)
        woper.write(sep.join([header1[:-1], append_header2]))
        # 右边样本总数
        right_sample_ct = len(append_header2.split(sep))
        # 左边样本总数
        left_sample_ct = len(append_header1.split(sep))
        while True:
            try:
                merge_datas, flag = compare_next(left_oper=left_open,
                                                 right_oper=right_open,
                                                 sep=sep,
                                                 sep_pos_ct=sep_pos_ct,
                                                 right_sample_ct=right_sample_ct,
                                                 left_sample_ct=left_sample_ct,
                                                 how=how)

                if merge_datas:
                    woper.write('\n')
                    woper.write('\n'.join(merge_datas))
                if flag:
                    break
            except OSError:
                break
        return output_path


def merge_sorted_snp_dir(input_dir=None,
                         verbose=False,
                         header_index_str='id,chrom,position,ref',
                         sep=',',
                         suffix=".csv",
                         output_dir_base=None,
                         remove_flag=False):
    """
    合同排序后的文件目录
    :param input_dir:  输入目录
    :param verbose:
    :param header_index_str: 索引头
    :param sep: 分隔符号
    :param suffix: 输入目录需要合并文件的后缀
    :param output_dir_base: 输出基目录
    :param remove_flag: 是否删除中间文件
    :return:
    """
    paths = [os.path.join(input_dir, _) for _ in os.listdir(input_dir) if _.endswith(suffix)]
    if len(paths) == 1:
        return paths[0]
    output_dir = tempfile.mkdtemp(prefix="merge_", dir=output_dir_base)
    logger.info(f"output_dir:{output_dir}")
    middle = int(len(paths) / 2)
    with ProcessPoolExecutor() as executor:
        to_do_map = {}
        itors = zip_longest(paths[0:middle], paths[middle:])
        for itor in itors:
            output_path = tempfile.mktemp(dir=output_dir, suffix=suffix)
            left_path, right_path = itor
            if left_path and right_path:
                future = executor.submit(merge_file, left_path, right_path, output_path, header_index_str, sep)
                to_do_map[future] = itor
            elif left_path is None and right_path:
                shutil.copy(right_path, output_path)
            elif right_path is None and left_path:
                shutil.copy(left_path, output_path)
        done_itor = futures.as_completed(to_do_map)
        if not verbose:
            done_itor = tqdm.tqdm(done_itor, total=len(to_do_map.keys()))
        for future in done_itor:
            future.result()
        if remove_flag:
            logger.warning(f"remove dir: {input_dir}")
            shutil.rmtree(input_dir)
        return merge_sorted_snp_dir(input_dir=output_dir, verbose=False, header_index_str=header_index_str, sep=sep, output_dir_base=output_dir_base, remove_flag=True)


def _sorted_file(path, sep, by, output_path):
    """
    根据索引排序文件
    :param path:
    :param sep:
    :param indexs:
    :param output_path:
    :return:
    """
    df = pd.read_csv(path, sep=sep, low_memory=False)
    sorted_df = df.sort_values(by=by)
    sorted_df.to_csv(output_path, index=False)
    return output_path


def merge_snp_dir(input_dir=None, sep=',', suffix=".csv", header_index_str='id,chrom,position,ref', verbose=False):
    """
    合并snp目录下的所有文件 文件可以不排序
    :return:
    """
    header_index_str = header_index_str.rstrip(sep)
    indexs = header_index_str.split(sep)
    paths = [os.path.join(input_dir, _) for _ in os.listdir(input_dir) if _.endswith(suffix)]
    output_dir = tempfile.mkdtemp(prefix="sorted_")
    logger.info(f"Sorted input_dir: {input_dir} to {output_dir} ...")
    ##########
    with ProcessPoolExecutor() as executor:
        to_do_map = {}
        for path in paths:
            output_path = tempfile.mktemp(dir=output_dir, suffix=suffix)
            future = executor.submit(_sorted_file, path, sep, indexs, output_path)
            to_do_map[future] = path
        done_itor = futures.as_completed(to_do_map)
        if not verbose:
            done_itor = tqdm.tqdm(done_itor, total=len(to_do_map.keys()))
        for future in done_itor:
            future.result()
    ##########
    logger.info(f"Merge input_dir: {input_dir} to  ...")
    output_dir_base = tempfile.mkdtemp(prefix="merge_root_")
    return merge_sorted_snp_dir(input_dir=output_dir, verbose=False, header_index_str=header_index_str, sep=sep, suffix=suffix, output_dir_base=output_dir_base)


def merge_snp_paths(paths, output=None, sep=',', suffix=".csv", header_index_str='id,chrom,position,ref', verbose=False):
    """
    合并snp目录下的所有文件 文件可以不排序
    :return:
    """
    header_index_str = header_index_str.rstrip(sep)
    indexs = header_index_str.split(sep)
    output_dir = tempfile.mkdtemp(prefix="sorted_")
    logger.info(f"Sorted paths: {len(paths)} to {output_dir} ...")
    with ProcessPoolExecutor() as executor:
        to_do_map = {}
        for path in paths:
            output_path = tempfile.mktemp(dir=output_dir, suffix=suffix)
            future = executor.submit(_sorted_file, path, sep, indexs, output_path)
            to_do_map[future] = path
        done_itor = futures.as_completed(to_do_map)
        if not verbose:
            done_itor = tqdm.tqdm(done_itor, total=len(to_do_map.keys()))
        for future in done_itor:
            future.result()
    logger.info(f"Merge input_dir: {output_dir} to  ...")
    output_dir_base = tempfile.mkdtemp(prefix="merge_root_")
    result_path = merge_sorted_snp_dir(input_dir=output_dir, verbose=False, header_index_str=header_index_str, sep=sep, suffix=suffix, output_dir_base=output_dir_base)
    if output:
        shutil.copy(result_path, output)
        return output
    else:
        return result_path
