import xarray as xr
import numpy as np
import os
import arrow
import pandas as pd
import metpy.calc as mpcalc
from metpy.units import units
import glob
# 先取得sstk 的 time
# 载入所有的dataset
# 遍历fcHour
# 提取各要素相同fchour的数据
# 合并多个array, xr.merge([ds/da])
# DataArray.to_series(),再合并pd.DataFrame({var:da.to_series})
# 输出各values to array
# https://docs.xarray.dev/en/stable/user-guide/combining.html#merge
# https://zhuanlan.zhihu.com/p/518580805
# 计算time offset
# 转换成pandas数据
# 根据时间检索出实况的能见度数据作为检验合并入其中df_G7425.loc[df_G7425.index[0]]
# 导出 csv 数据，每个起报时间csv文件

fcHour_list = np.array(list(range(0, 72+1, 3)) + list(range(78, 240+1, 6)))
fcHour_list_target = np.array(range(0, 240+1, 1))
# fcHour_list = list(range(0, 72+1, 3)) + list(range(78, 168+1, 6))
single_level_name_list = ['visi','v100','v10m','u100','u10m','t2mm','t2md','sstk','sktk','mx2t','mn2t']
multi_level_name_list = ['vwnd','uwnd','temp','rhum']
levels_selected = [1000.0, 950.0, 925.0, 900.0, 850.0]

multi_level_columns = []
for multi_name_key in multi_level_name_list:
    for iLevel in levels_selected:
        multi_level_columns.append(f'{multi_name_key}{int(iLevel)}')

dir_path = 'F:/github/pythonScript/seafog/data/netcdf/CFdata/'

def get_time_list(ds):
    da_time = ds['time']
    return da_time

def get_elems():
    pass

def fill_hour(filepath, file_name, name):
    if name in single_level_name_list:
        output_path = os.path.join(dir_path, './fullhour', f'fullhour.{file_name}')
        if os.path.exists(output_path):
            print('文件已存在'+output_path)
            return # skip
        ids = xr.open_dataset(os.path.join(filepath, file_name))

        for iHour_target in fcHour_list_target:
            if(iHour_target in fcHour_list): 
                continue
            else:
                # 寻找最近的两个值
                hour_distance = fcHour_list - iHour_target
                left_shift  = np.max(hour_distance[hour_distance<0])
                right_shift = np.min(hour_distance[hour_distance>0])
                varname = f'{name}{iHour_target:0>3d}'
                left_hour = iHour_target + left_shift
                right_hour = iHour_target + right_shift 
                varname_left = f'{name}{left_hour:0>3d}'
                varname_right = f'{name}{right_hour:0>3d}'
                if(name == 'sstk'):
                    ids[varname_left] = xr.where(ids[varname_left] < 400, ids[varname_left], np.nan)
                    ids[varname_right] =  xr.where(ids[varname_right] < 400, ids[varname_right], np.nan)

                with xr.set_options(keep_attrs=True):
                    ids[varname] = (ids[varname_left]*(right_hour-iHour_target) + ids[varname_right]*(iHour_target-left_hour))/(right_hour-left_hour)
        
        ids.to_netcdf(output_path)
        print(f'扩增完成{name}')


    if name in multi_level_name_list:
        output_path = os.path.join(dir_path, './fullhour', f'fullhour.{file_name}')
        if os.path.exists(output_path):
            print('文件已存在'+output_path)
            return # skip
        ids = xr.open_dataset(os.path.join(filepath, file_name))

        for iHour_target in fcHour_list_target:
            if(iHour_target in fcHour_list): 
                continue
            else:
                # 寻找最近的两个值
                hour_distance = fcHour_list - iHour_target
                left_shift  = np.max(hour_distance[hour_distance<0])
                right_shift = np.min(hour_distance[hour_distance>0])
                varname = f'{name}{iHour_target:0>3d}'
                left_hour = iHour_target + left_shift
                right_hour = iHour_target + right_shift 
                varname_left = f'{name}{left_hour:0>3d}'
                varname_right = f'{name}{right_hour:0>3d}'
                with xr.set_options(keep_attrs=True):
                    ids[varname] = (ids[varname_left]*(right_hour-iHour_target) + ids[varname_right]*(iHour_target-left_hour))/(right_hour-left_hour)
        
        ids.to_netcdf(output_path)
        print(f'扩增完成{name}')

def scan_netcdf(inputDirpath):
    reg = inputDirpath+'*.nc'
    path_list = glob.glob(reg)
    for iPath in path_list:
        (filepath,filename) = os.path.split(iPath)
        print(filename)
        name = iPath[-7:-3]
        fill_hour(filepath,filename,name)

scan_netcdf(dir_path)
# fill_hour()