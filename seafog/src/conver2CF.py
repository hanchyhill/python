import xarray as xr
import numpy as np
import os
import glob
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
    'sktk':{
        'name':'sktk',
        'missing_value': -999.9,
        # '_FillValue':  -999.9,
        'valid_min': 0.0,
        'standard_name': 'surface_temperature',
        'units': 'K',
        'long_name': 'surface temperature',
        'short_name': 'sktk',
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
single_level_name_list = ['visi','v100','v10m','u100','u10m','t2mm','t2md','sstk','sktk','mx2t','mn2t']
multi_level_name_list = ['vwnd','uwnd','temp','rhum']


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
    ds = xr.open_dataset(os.path.join(dirPath,file))
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
    '''
    这是一个函数，用于将多个NetCDF文件合并为一个文件。函数输入参数包括文件名列表、新文件名和文件路径。函数使用xarray库打开文件，
    并使用concat()函数根据时间维度将数据集连接起来，并将结果输出到一个新的NetCDF文件中。
    '''
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
    '''
    这段 Python 代码定义了一个函数 
    concat_multi_ds
    ，它的作用是将指定文件夹下的多个 NetCDF 文件按照变量名进行合并,合并后包含同一经纬度上的各个变量。函数接受四个参数：
    
    name_list：一个列表，包含需要合并的变量名。
    file_prefix_01：一个字符串，表示第一个时间段文件名的前缀。
    file_prefix_02：一个字符串，表示第二个时间段文件名的前缀。
    new_file_prefix：一个字符串，表示合并后文件的前缀。
    在函数内部，对于每个变量名，先构造出两个时间段的文件名和合并后文件的文件名，然后调用外部函数 
    concat2DataSets
     来实现数据集的合并。最终会输出合并的进度信息。
    '''
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

def scan_convet_CF(inputDirpath):
    reg = inputDirpath+'*.nc'
    path_list = glob.glob(reg)
    for iPath in path_list:
        (filepath,filename) = os.path.split(iPath)
        print(filename)
        output_path = os.path.join(inputDirpath, './CFdata',filename[6:])
        if os.path.exists(output_path):
            print('文件已存在'+output_path)
            continue # skip
        
        name = iPath[-7:-3]
        if name in single_level_name_list:
            dataset = openDataSet(filepath, filename, name, True)
        elif name in multi_level_name_list:
            dataset = openDataSet(filepath, filename, name, False)
        else:
            raise ValueError("未检索到文件名"+name)
        
        dataset.to_netcdf(output_path)
        dataset.close()



# conver_nc_CF()
# conver_nc_CF(['v100','v10m','u100','u10m'],['vwnd','uwnd'],'201810-202103_lon110.25_lat20.125.','201810-202103_lon110.25_lat20.25.')


# concat_multi_ds()
# concat_multi_ds(['vwnd','uwnd','temp','rhum'], '201810-202103_lon110.25_lat20.25.','202104-202205_lon110.25_lat20.25.','201810-202205_lon110.25_lat20.25.','H:/github/python/seafog/data/CFdata/')

# conver_nc_CF(['v10m','u10m','u100','v100' , 'visi', 'sstk', 't2mm', 't2md','sktk'],[],'59754.201508-201809_lon110.25_lat20.125.','59754.201508-201809_lon110.25_lat20.25.')
# concat_multi_ds(['v10m','u10m','u100','v100' , 'visi', 'sstk', 't2mm', 't2md','sktk'], '59754.201508-201809_lon110.25_lat20.125.','201810-202205_lon110.25_lat20.125.','201508-202205_lon110.25_lat20.125.','H:/github/python/seafog/data/CFdata/')
# conver_nc_CF(['sktk'],[],'59754.201810-202205_lon110.25_lat20.125.','59754.201508-201809_lon110.25_lat20.25.')
# concat_multi_ds(['sktk'], '59754.201508-201809_lon110.25_lat20.125.','59754.201810-202205_lon110.25_lat20.125.','201508-202205_lon110.25_lat20.125.','H:/github/python/seafog/data/CFdata/')
# conver_nc_CF([],['vwnd','uwnd','temp','rhum'],'59754.201810-202205_lon110.25_lat20.25.','59754.201508-201809_lon110.25_lat20.25.')
# concat_multi_ds(['vwnd','uwnd','temp','rhum'], '59754.201508-201809_lon110.25_lat20.25.','201810-202205_lon110.25_lat20.25.','201508-202205_lon110.25_lat20.25.','H:/github/python/seafog/data/CFdata/')
scan_convet_CF('F:/github/pythonScript/seafog/data/netcdf/')