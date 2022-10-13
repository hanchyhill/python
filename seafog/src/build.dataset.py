import xarray as xr
import numpy as np
import os
import arrow
import pandas as pd

# 对能见度分类为2的抽取50%值
# 按需加载预报时效的数据
# 合并所有数据
# 分类雾，轻雾，无雾天气
# 训练集train, 测试集valid, 验证集test分组
# DataFrame.sample(n=None, frac=None, replace=False, weights=None, random_state=None, axis=None)
# fcHour_list = list(range(0, 72+1, 3)) + list(range(78, 240+1, 6))
# fcHour_list = list(range(0, 72+1, 3)) + list(range(78, 168+1, 6))