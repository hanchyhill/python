import xarray as xr
import numpy as np
import os

config = {
    'sstk':{
        'name':'sstk',
        'missing_value': -999.9,
        # '_FillValue':  -999.9,
        'valid_min': 0.0,
        'standard_name': 'sea_surface_skin_temperature',
        'units': 'K',
        'long_name': 'sea surface temperature',
        'short_name': 'SST',
    },
    'visi':{
        'name':'visi',
        'missing_value': -999.9,
        # '_FillValue':  -999.9,
        'valid_min': 0.0,
        'standard_name': 'visibility_in_air',
        'units': 'm',
        'long_name': 'visibility',
        'short_name': 'visibility',
    },
    't2md':{
        'name':'t2md',
        'missing_value': -999.9,
        # '_FillValue':  -999.9,
        'valid_min': 0.0,
        'standard_name': 'dew_point_temperature',
        'units': 'K',
        'long_name': 'dew point',
        'short_name': 'Td',
    },
    't2mm':{
        'name':'t2mm',
        'missing_value': -999.9,
        # '_FillValue':  -999.9,
        'valid_min': 0.0,
        'standard_name': 'air_temperature',
        'units': 'K',
        'long_name': 'air temperature in 2 metre',
        'short_name': 'T2m',
    },
    'mn2t':{
        'name':'mn2t',
        'missing_value': -999.9,
        # '_FillValue':  -999.9,
        'valid_min': 0.0,
        'standard_name': 'air_temperature',
        'units': 'K',
        'long_name': 'air temperature minimum in 2 metre since 6 hr before',
        'short_name': 'T2m',
    },
    'mx2t':{
        'name':'mx2t',
        'missing_value': -999.9,
        # '_FillValue':  -999.9,
        'valid_min': 0.0,
        'standard_name': 'air_temperature',
        'units': 'K',
        'long_name': 'air temperature maxium in 2 metre since 6 hr before',
        'short_name': 'T2m',
    },
    'rhum':{
        'name':'rhum',
        'missing_value': -999.9,
        # '_FillValue':  -999.9,
        'valid_min': 0.0,
        'standard_name': 'relative_humidity',
        'units': '0.01',
        'long_name': 'relative_humidity',
        'short_name': 'RH',
    },
    'temp':{
        'name':'temp',
        'missing_value': -999.9,
        # '_FillValue':  -999.9,
        'valid_min': 0.0,
        'standard_name': 'air_temperature',
        'units': 'K',
        'long_name': 'air temperature',
        'short_name': 'Temp',
    },
    'uwnd':{
        'name':'uwnd',
        'missing_value': -999.9,
        # '_FillValue':  -999.9,
        'standard_name': 'eastward_wind',
        'units': 'm/s',
        'long_name': 'u wind',
        'short_name': 'Uwnd',
    },
    'vwnd':{
        'name':'vwnd',
        'missing_value': -999.9,
        # '_FillValue':  -999.9,
        'standard_name': 'northward_wind',
        'units': 'm/s',
        'long_name': 'v wind',
        'short_name': 'Vwnd',
    },
    'u10m':{
        'name':'u10m',
        'missing_value': -999.9,
        # '_FillValue':  -999.9,
        'standard_name': 'eastward_wind',
        'units': 'm/s',
        'long_name': 'u wind',
        'short_name': 'Uwnd',
    },
    'v10m':{
        'name':'v10m',
        'missing_value': -999.9,
        # '_FillValue':  -999.9,
        'standard_name': 'northward_wind',
        'units': 'm/s',
        'long_name': 'v wind',
        'short_name': 'Vwnd',
    },
    'u100':{
        'name':'u100',
        'missing_value': -999.9,
        # '_FillValue':  -999.9,
        'standard_name': 'eastward_wind',
        'units': 'm/s',
        'long_name': 'u wind',
        'short_name': 'Uwnd',
    },
    'v100':{
        'name':'v100',
        'missing_value': -999.9,
        # '_FillValue':  -999.9,
        'standard_name': 'northward_wind',
        'units': 'm/s',
        'long_name': 'v wind',
        'short_name': 'Vwnd',
    },
}

file0 = '201810-202101_lon110.25_lat20.125.sstk.nc'
file1 = '202102-202103_lon110.25_lat20.125.sstk.nc'
dirPath = 'H:/github/python/seafog/data/'

# ds.fillna(value=values)
# da.where(x < 0, np.nan)
# da.fillna(-999.9)

def createVarNameList(varNamePrefix = 'sstk'):
    timeList = list(range(0, 72+1, 3)) + list(range(78, 240+1, 6))
    varNameList = list(map(lambda time: '{}{:0>3d}'.format(varNamePrefix, time), timeList))
    return varNameList

def convertDataArray(dataArray, daConfig, dropLevel: True):
    '''
    转换格式
    @dropLevel: 是否删除最后一个维度, 地面数据会多出一个level维度
    '''
    if(dropLevel):
        dataArray = dataArray[:,0]
    if('valid_min' in daConfig):
        dataArray = xr.where(dataArray>daConfig['valid_min'], dataArray,np.nan)
    # dataArray.fillna(daConfig['missing_value'])
    
    dataArray = xr.where(dataArray<=daConfig['missing_value'], np.nan, dataArray)
    dataArray.attrs = daConfig
    del dataArray.attrs['missing_value']
    return dataArray


def openDataSet(dirPath = 'H:/github/python/seafog/data/', file = '201810-202101_lon110.25_lat20.125.sstk.nc',varNameSuffix='sstk',dropLevel=False):
    ds = xr.open_dataset(dirPath + file)
    iConfig = config[varNameSuffix]
    nameList = createVarNameList(iConfig['name'])
    for varName in nameList:
        ds[varName] = convertDataArray(ds[varName], iConfig, dropLevel)
    return ds

def concatDataSet():
    name = 't2md'
    dataset01 = openDataSet(dirPath, '201810-202101_lon110.25_lat20.125.t2md.nc', name)
    dataset02 = openDataSet(dirPath, '202102-202103_lon110.25_lat20.125.t2md.nc', name)
    newDataset = xr.concat([dataset01, dataset02], dim="time")
    newDataset.to_netcdf('201810-202103_lon110.25_lat20.125.{}.nc'.format(name))

def openPressureDataSet():
    name = 'rhum'
    dataset = openDataSet(dirPath, '201810-202103_lon110.25_lat20.25.rhum.nc', name, False)
    dataset.to_netcdf('201810-202103_lon110.25_lat20.25.{}.nc'.format(name))


def concat2DataSets(name, file_name_list:list, new_file_name:str, dirPath = 'H:/github/python/seafog/data/CFdata/'):
    dataset_list = []
    for file_name in file_name_list:
        # ds = openDataSet(dirPath, file_name, name, False)
        ds = xr.open_dataset(os.path.join(dirPath, file_name))
        dataset_list.append(ds)
    newDataset = xr.concat(dataset_list, dim="time")
    newDataset.to_netcdf(new_file_name)

def concat_multi_ds(
    name_list = ['visi','v100','v10m','u100','u10m','t2mm','t2md','sstk'],
    file_prefix_01 = '201810-202103_lon110.25_lat20.125.',
    file_prefix_02 = '202104-202205_lon110.25_lat20.125.',
    new_file_prefix = '201810-202205_lon110.25_lat20.125.',
    dirPath = 'H:/github/python/seafog/data/CFdata/',
    ):
    for name in name_list:
        print(f'合并{name}')
        file01 = f'{file_prefix_01}{name}.nc'
        file02 = f'{file_prefix_02}{name}.nc'
        new_file = f'{new_file_prefix}{name}.nc'
        concat2DataSets(name, [file01, file02], new_file, dirPath)

# openPressureDataSet()
def conver_nc_CF(
    single_level_name_list = ['visi','v100','v10m','u100','u10m','t2mm','t2md','sstk'],
    multi_level_name_list = ['vwnd','uwnd','temp','rhum'],
    single_file_prefix='202104-202205_lon110.25_lat20.125.',
    pressure_file_prefix='202104-202205_lon110.25_lat20.25.',
    ):
    '''
    转换为合适的CF格式
    @single_level_name_list: 单层数据文件
    @multi_level_name_list: 气压层数据文件
    @single_file_prefix: 单层文件名前缀
    @pressure_file_prefix: 气压层文件名前缀
    '''
    for name in single_level_name_list:
        file_name = f'{single_file_prefix}{name}.nc'
        print(f'转换文件{file_name}')
        dataset = openDataSet(dirPath, file_name, name, True)
        dataset.to_netcdf(file_name)

    for name in multi_level_name_list:
        file_name = f'{pressure_file_prefix}{name}.nc'
        print(f'转换文件{file_name}')
        dataset = openDataSet(dirPath, file_name, name, False)
        dataset.to_netcdf(file_name)
# conver_nc_CF()
# conver_nc_CF(['v100','v10m','u100','u10m'],['vwnd','uwnd'],'201810-202103_lon110.25_lat20.125.','201810-202103_lon110.25_lat20.25.')
concat_multi_ds()
concat_multi_ds(['vwnd','uwnd','temp','rhum'], '201810-202103_lon110.25_lat20.25.','202104-202205_lon110.25_lat20.25.','201810-202205_lon110.25_lat20.25.','H:/github/python/seafog/data/CFdata/')