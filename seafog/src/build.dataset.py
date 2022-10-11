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