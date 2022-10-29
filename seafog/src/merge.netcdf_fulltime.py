import xarray as xr
import numpy as np
import os
import arrow
import pandas as pd
import metpy.calc as mpcalc
from metpy.units import units

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
if os.environ['COMPUTERNAME'] == 'DESKTOP-EQAO3M5':
  computer_flag = 'home'
else:
  computer_flag = 'office'

if computer_flag == 'home':
  file_dir = "F:/github/pythonScript/seafog/"
else:
  file_dir = "H:/github/python/seafog/"

fcHour_list = np.array(range(0, 240+1, 1))# list(range(0, 72+1, 3)) + list(range(78, 240+1, 6))
# fcHour_list = list(range(0, 72+1, 3)) + list(range(78, 168+1, 6))
single_level_name_list = ['visi','v100','v10m','u100','u10m','t2mm','t2md','sstk']
multi_level_name_list = ['vwnd','uwnd','temp','rhum']
levels_selected = [1000.0, 950.0, 925.0, 900.0, 850.0]

multi_level_columns = []
for multi_name_key in multi_level_name_list:
    for iLevel in levels_selected:
        multi_level_columns.append(f'{multi_name_key}{int(iLevel)}')

dir_path = os.path.normpath(os.path.join(file_dir, './data/CFdata/concat/201508-202205/'))

file_59754 = os.path.normpath(os.path.join(file_dir, './data/station/59754.20130101-2022093023.csv'))
df_59754 = pd.read_csv(file_59754,sep=',',na_values=[9999])
df_59754.index = pd.to_datetime(df_59754["DDATETIME"])
# print(df_59754.dtypes)

h5_store = pd.HDFStore(os.path.normpath(os.path.join(file_dir, './data/collection/fullhour/full.suf110.25_20.125_pre110.25_20.25_59754_201508-202205.hdf')), mode='w')

def get_time_list(ds):
    da_time = ds['time']
    return da_time

def get_align_time(ds_list):
    align_time = ds_list[0]['time']
    for i_ds in ds_list:
        i_time = i_ds['time']
        align_time, _ = xr.align(align_time, i_time)
    return align_time

def get_elems():
    pass

def convert_nc2csv():
    file_name = 'fullhour.201508-202205_lon110.25_lat20.125.sstk.nc'
    ds_time = xr.open_dataset(os.path.join(dir_path, file_name))
    # init_time = pd.Series(ds_time['time'])
    ds_single_list = []
    
    for name in single_level_name_list:
        file_name = f'fullhour.201508-202205_lon110.25_lat20.125.{name}.nc'
        ids = xr.open_dataset(os.path.join(dir_path, file_name))
        ds_single_list.append(ids)

    ds_multi_list = []
    
    for name in multi_level_name_list:
        file_name = f'fullhour.201508-202205_lon110.25_lat20.25.{name}.nc'
        ids = xr.open_dataset(os.path.join(dir_path, file_name))
        ds_multi_list.append(ids)
    ds_all = ds_single_list + ds_multi_list
    init_time = get_align_time(ds_all)
    ds_t = ds_multi_list[multi_level_name_list.index('temp')]
    ds_rh = ds_multi_list[multi_level_name_list.index('rhum')]

    for iHour in fcHour_list:
        print(iHour)
        series_map = dict()
        for index in range(len(single_level_name_list)):
            name_key = single_level_name_list[index]
            i_ds = ds_single_list[index]
            i_varname = f'{name_key}{iHour:0>3d}'
            da = i_ds[i_varname].sel(time = init_time)
            series_map[name_key] = da.values
        # TODO: temp. and rhum in 850hPa and 925hPa 计算逆温情况
        for index in range(len(multi_level_name_list)):
            name_key = multi_level_name_list[index]
            i_ds = ds_multi_list[index]
            i_varname = f'{name_key}{iHour:0>3d}'
            da = i_ds[i_varname].sel(time = init_time)
            for iLevel in levels_selected:
                iVarname = f'{name_key}{int(iLevel)}'
                series_map[iVarname] = da.sel(level = iLevel).values
                if(name_key == 'temp'): series_map[iVarname] = series_map[iVarname] - 273.15

            # series_map[name_key] = da.values
        # TODO: 计算相当位温
        
        da_t  = ds_t[f'temp{iHour:0>3d}'].sel(time = init_time)
        da_rh = ds_rh[f'rhum{iHour:0>3d}'].sel(time = init_time)
        for iLevel in levels_selected:
            iVarname = f'theta_e{int(iLevel)}'
            rhum = np.clip(da_rh.sel(level = iLevel), 0, 100)*units.percent
            temp = da_t.sel(level = iLevel)*units.kelvin
            td = mpcalc.dewpoint_from_relative_humidity(temp, rhum)
            level_list = np.full_like(temp,iLevel)
            theta_e = mpcalc.equivalent_potential_temperature(level_list*units.hPa, temp, td)
            # theta_e = theta_e.to(units.degC)
            series_map[iVarname] = theta_e.values - 273.15

            iVarname = f'theta{int(iLevel)}'
            theta = mpcalc.potential_temperature(level_list*units.hPa, temp)
            series_map[iVarname] = theta.values - 273.15
            


        df = pd.DataFrame(series_map)
        df['actual_time'] = init_time + pd.Timedelta(iHour,unit='h')
        df['init_time'] = init_time
        df['year'] =  df['actual_time'].dt.year
        df['month'] =  df['actual_time'].dt.month
        df['day'] =  df['actual_time'].dt.day
        df['hour'] =  df['actual_time'].dt.hour
        df['year_sin'] = np.sin((df['actual_time'].dt.dayofyear / 365.25) * 2 * np.pi)
        df['year_cos'] = np.cos((df['actual_time'].dt.dayofyear / 365.25) * 2 * np.pi)
        df['day_sin'] = np.sin((df['hour'] / 24) * 2 * np.pi)
        df['day_cos'] = np.cos((df['hour'] / 24) * 2 * np.pi)
        df['t2mm'] = df['t2mm'] - 273.15
        df['t2md'] = df['t2md'] - 273.15
        df['sstk'] = df['sstk'] - 273.15
        df['station_vis'] = get_station_data_by_time(df['actual_time'],'V20059')
        # df['station_vis'] = get_station_data_by_time(df['actual_time'],'V20001')
        df['station_rain1'] = get_station_data_by_time(df['actual_time'],'V13019')
        df['fc_hour'] = iHour
        df.dropna(subset=['v100','v10m','u100','u10m','t2mm','t2md','sstk','station_vis'],inplace=True)
        df.dropna(subset=multi_level_columns,inplace=True)
        df['station_vis_cat'] = 3
        df.loc[df['station_vis']<=1000,('station_vis_cat')] = 0
        df.loc[(df['station_vis']>1000) & (df['station_vis']<=10000),'station_vis_cat'] = 1
        df.loc[df['station_vis']>10000,'station_vis_cat'] = 2
        df_rain_remove = df[df['station_rain1']<1]
        df_rain_remove.to_hdf(h5_store, key=f'df_{iHour:0>3d}', mode='a')
        df_rain_remove.to_csv(f'{dir_path}ec201508-202205_xuwen_{iHour:0>3d}.csv')
    h5_store.close()

def get_station_data_by_time(time_list, key):
    a_nan = np.empty(len(time_list))
    a_nan.fill(np.nan)
    da = pd.Series(a_nan)
    for index in range(len(time_list)):
        iTime = time_list[index]
        if iTime in df_59754.index:
            da[index] = df_59754.loc[iTime][key]
    return da

convert_nc2csv()