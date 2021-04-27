import xarray as xr
import numpy as np

config = {
    'sstk':{
        'name':'sstk',
        'missing_value': -999.9,
        '_FillValue':  -999.9,
        'valid_min': 0.0,
        'standard_name': 'sea_surface_skin_temperature',
        'units': 'K',
        'long_name': 'sea surface temperature',
        'short_name': 'SST',
    },
    'visi':{
        'name':'visi',
        'missing_value': -999.9,
        '_FillValue':  -999.9,
        'valid_min': 0.0,
        'standard_name': 'visibility_in_air',
        'units': 'm',
        'long_name': 'visibility',
        'short_name': 'visibility',
    },
    't2md':{
        'name':'t2md',
        'missing_value': -999.9,
        '_FillValue':  -999.9,
        'valid_min': 0.0,
        'standard_name': 'dew_point_temperature',
        'units': 'K',
        'long_name': 'dew point',
        'short_name': 'Td',
    },
    't2mm':{
        'name':'t2mm',
        'missing_value': -999.9,
        '_FillValue':  -999.9,
        'valid_min': 0.0,
        'standard_name': 'air_temperature',
        'units': 'K',
        'long_name': 'air temperature in 2 metre',
        'short_name': 'T2m',
    },
    'rhum':{
        'name':'rhum',
        'missing_value': -999.9,
        '_FillValue':  -999.9,
        'valid_min': 0.0,
        'standard_name': 'relative_humidity',
        'units': '0.01',
        'long_name': 'relative_humidity',
        'short_name': 'RH',
    },
    'temp':{
        'name':'temp',
        'missing_value': -999.9,
        '_FillValue':  -999.9,
        'valid_min': 0.0,
        'standard_name': 'air_temperature',
        'units': 'K',
        'long_name': 'air temperature',
        'short_name': 'Temp',
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
    '''
    if(dropLevel):
        dataArray = dataArray[:,0]
    dataArray = xr.where(dataArray>daConfig['valid_min'], dataArray,np.nan)
    dataArray.fillna(daConfig['missing_value'])
    dataArray.attrs = daConfig
    return dataArray
    pass

def openDataSet(dirPath = 'H:/github/python/seafog/data/', file = '201810-202101_lon110.25_lat20.125.sstk.nc',varNameSuffix='sstk',dropLevel=True):
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

openPressureDataSet()