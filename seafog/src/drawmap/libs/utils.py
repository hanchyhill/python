import xarray as xr
import numpy as np
nwp_var_config = {
    
    'sstk':{
        'name':'sstk',
        'missing_value': -999.9,
        # '_FillValue':  -999.9,
        'valid_min': 273.161,
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
    'fog':{
        'name':'fog',
        'valid_min': 0.0,
        'missing_value': -999.9,
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

def convertDataArray(dataArray, daConfig, dropLevel =  False):
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
    dataArray.attrs = daConfig.copy()
    del dataArray.attrs['missing_value']
    return dataArray


def findLogDataBytimeStep(info, timeStep):
    response = False
    for item in info:
        if(item['timeStep'] == timeStep):
            response = item
            break
    return response

def findLogDataByStationId(info, station_id):
    '''
    根据station ID 查找日志记录
    '''
    response = False
    for item in info:
        if(item['stationId'] == station_id):
            response = item
            break
    return response


def readFromTDS_seafog(initTime: str = '2022031700', modelId: str = 'ecmwfthin', area: list = [105, 125, 15, 28]) -> dict:
    '''
    从TDS接口读取指定起报时间数据, 返回dataset
    :params initTime: 起报时间世界时YYYYMMDDHH
    :params modelId: 模式名
    :params area: 筛选区域[西, 东, 南, 北]
    :return dataset字典
    '''
    # 需要读取的要素t2mm, t2md, sstk, v100, v10m, u100, u10m, (rhum, temp) => (theta925,theta1000)
    # TODO: 高空的数据插值到地面的分辨率上
    year = initTime[0:4]
    month = initTime[4:6]
    day = initTime[6:8]
    hour = initTime[8:10]
    selectedTime = '{0}-{1}-{2} {3}:00:00'.format(year, month, day, hour)
    baseUrl = 'http://10.148.8.71:7080/thredds/dodsC/{0}/'.format(modelId)
    url_td = baseUrl + f'{year}{month}/t2md.nc'
    url_t2m = baseUrl + f'{year}{month}/t2mm.nc'
    url_sst = baseUrl + f'{year}{month}/sstk.nc'
    url_u100 = baseUrl + f'{year}{month}/u100.nc'
    url_v100 = baseUrl + f'{year}{month}/v100.nc'
    url_u10m = baseUrl + f'{year}{month}/u10m.nc'
    url_v10m = baseUrl + f'{year}{month}/v10m.nc'
    url_rhum = baseUrl + f'{year}{month}/rhum.nc'
    url_temp = baseUrl + f'{year}{month}/temp.nc'
    try:
        dataSet_td = xr.open_dataset(url_td)
        dataSet_t2m = xr.open_dataset(url_t2m)
        dataSet_sst = xr.open_dataset(url_sst)
        dataSet_u100 = xr.open_dataset(url_u100)
        dataSet_v100 = xr.open_dataset(url_v100)
        dataSet_u10m = xr.open_dataset(url_u10m)
        dataSet_v10m = xr.open_dataset(url_v10m)
        dataSet_rhum = xr.open_dataset(url_rhum)
        dataSet_temp = xr.open_dataset(url_temp)
    except Exception as e:
        print('无法获取数据源')
        raise e
    ds_td = dataSet_td.sel(time=selectedTime, level=0.0, lat=slice(
        area[2], area[3]), lon=slice(area[0], area[1]))
    ds_t2m = dataSet_t2m.sel(time=selectedTime, level=0.0, lat=slice(
        area[2], area[3]), lon=slice(area[0], area[1]))
    ds_sst = dataSet_sst.sel(time=selectedTime, level=0.0, lat=slice(
        area[2], area[3]), lon=slice(area[0], area[1]))
    ds_u100 = dataSet_u100.sel(time=selectedTime, level=0.0, lat=slice(
        area[2], area[3]), lon=slice(area[0], area[1]))
    ds_v100 = dataSet_v100.sel(time=selectedTime, level=0.0, lat=slice(
        area[2], area[3]), lon=slice(area[0], area[1]))
    ds_u10m = dataSet_u10m.sel(time=selectedTime, level=0.0, lat=slice(
        area[2], area[3]), lon=slice(area[0], area[1]))
    ds_v10m = dataSet_v10m.sel(time=selectedTime, level=0.0, lat=slice(
        area[2], area[3]), lon=slice(area[0], area[1]))
    ds_rhum = dataSet_rhum.sel(time=selectedTime, level=[1000.0, 925.0], lat=slice(
        area[2], area[3]), lon=slice(area[0], area[1]))
    ds_temp = dataSet_temp.sel(time=selectedTime, level=[1000.0, 925.0], lat=slice(
        area[2], area[3]), lon=slice(area[0], area[1]))
    
    return {'td': ds_td, 't2m': ds_t2m, 'sst': ds_sst, 'u100':ds_u100, 'v100':ds_v100, 'u10m':ds_u10m,
            'v10m':ds_v10m, 'rhum':ds_rhum, 'temp':ds_temp}
