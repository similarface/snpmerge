# snps

tools for merging SNPs files
> 在生产中可能需要合并非常多的snp文件为一个big snp，合并后文件太大，使用pandas方式可能出现内存OOM。

# Features
- 多进程
- 低内存
- 依赖少

# Supported Genotype Files

```text
id,chrom,position,ref,sample_01,sample_02
1_161014,1,161014,C,CC,CC
1_184556,1,184558,A,AC
1_184558,1,184558,A,AC,CC
1_239523,1,239523,C,CA,CC
1_261794,1,261794,C,CC,CC
```

    +

```text
id,chrom,position,ref,sample_03
1_161014,1,161014,C,CC
1_184557,1,184558,A,AC
1_184558,1,184558,A,AC
1_239523,1,239523,C,CA
1_261794,1,261794,C,CC
```

    =

```text
id,chrom,position,ref,sample_03,sample_01,sample_02
1_161014,1,161014,C,CC,CC,CC
1_184558,1,184558,A,AC,AC,CC
1_239523,1,239523,C,CA,CA,CC
1_261794,1,261794,C,CC,CC,CC
```

- logger

```text
2022-06-20 11:08:00.084 | INFO     | __main__:merge_snp_dir:347 - Sorted input_dir: ./data/ to /var/folders/s_/bxn4wl393zggmkrmznqwfqn40000gn/T/sorted_p_42wx7u ...
100%|██████████| 2/2 [00:00<00:00,  4.91it/s]
2022-06-20 11:08:00.570 | INFO     | __main__:merge_snp_dir:361 - Merge input_dir: ./data/ to  ...
2022-06-20 11:08:00.571 | INFO     | __main__:merge_sorted_snp_dir:297 - output_dir:/var/folders/s_/bxn4wl393zggmkrmznqwfqn40000gn/T/merge_root__zqyn0kz/merge_x8pym7cw
100%|██████████| 1/1 [00:00<00:00,  2.99it/s]
0.8520910739898682 /var/folders/s_/bxn4wl393zggmkrmznqwfqn40000gn/T/merge_root__zqyn0kz/merge_x8pym7cw/tmpuiwtqodx.csv
```

- merge many file

### 使用方法

#### 安装
```bash 
pip install snpmerge==0.1.0
```


#### 目录输入

```python
from snpmerge.mul import merge_snp_dir

snp_dir = "./data"
output = "./merge_result.csv"
result = merge_snp_dir(input_dir=snp_dir, sep=',', suffix=".csv", header_index_str='id,chrom,position,ref', output=output)
```

#### paths输入

```python
paths = ['./data/f1.csv', './data/f2.csv', './data/f3.csv']
from snpmerge.mul import merge_snp_paths

result = merge_snp_paths(paths=paths, output=None, sep=',', suffix=".csv", header_index_str='id,chrom,position,ref')
```