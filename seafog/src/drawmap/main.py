import xarray as xr
import numpy as np
import libs.drawMap as drawTools
import os
import arrow
import json
import time
import pandas as pd
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
import datetime

timeStrList = ['000', '003', '006', '009', '012', '015', '018', '021', '024', '027', '030', '033', '036', '039', '042', '045', '048', '051', '054', '057', '060', '063', '066', '069', '072', '078', '084', '090',
               '096', '102', '108', '114', '120', '126', '132', '138', '144', '150', '156', '162', '168', '174', '180', '186', '192', '198', '204', '210', '216', '222', '228', '234', '240', ]
fcHour_list = list(range(0, 72+1, 3)) + list(range(78, 240+1, 6))

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

current_dir = os.path.dirname(__file__)
if(os.name == 'nt'):
    imgBaseDir = 'demo/'
    dataBaseDir = 'data/'
else:
    imgBaseDir = '../data/img/seafog/'
    dataBaseDir = '../data/img/seafog/'



def loadLog(time, type='map'):
    if(type=='map'):
        fileName = f'log_{time.format("YYYYMMDDHH")}.json'
    elif(type=='point'):
        fileName = f'log_timeSeries_{time.format("YYYYMMDDHH")}.json'
    else:
        print('请设置正确的日志type: ' + type)
        raise '请设置正确的日志type: ' + type
    
    logDir = os.path.join(
        current_dir, f'../{imgBaseDir}{time.format("YYYY/MM/DDHH/")}')
    logPath = os.path.join(logDir, fileName)
    logPath = os.path.normpath(logPath)
    isExists = os.path.exists(logPath)
    if(isExists):
        try:
            f_log = open(logPath, 'r')
            txt = f_log.read()
            info = {
                'success': True,
                'isExists': True,
                'data': txt,
            }
            f_log.close()
            return info
        except Exception as e:
            print('读取日志文件错误')
            info = {
                'success': False,
                'isExists': True,
                'message': 'Read Log File Error',
            }
            return info
    else:
        info = {
            'success': False,
            'isExists': False,
            'message': 'File Not Exists',
        }
        return info

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

def calLatestBaseTime() -> str:
    '''
    计算最新时次起报
    :return baseTime YYYYMMDDHH
    '''
    utcnow = arrow.utcnow()
    hour = utcnow.hour
    # ECMWF 任务计划https://confluence.ecmwf.int/display/UDOC/Dissemination+schedule
    if(hour >= 7 and hour < 19):
        baseTime = f"{utcnow.format('YYYYMMDD')}00"  # 世界时7~19时用当天00时起报
    elif (hour >= 19):
        baseTime = f"{utcnow.format('YYYYMMDD')}12"  # 世界时19~00时用当天00时起报
    else:
        # 小于世界时7时用前一天12时起报
        baseTime = f"{utcnow.shift(days = -1).format('YYYYMMDD')}12"
    return baseTime

def interpolate(dataArray,  resolution:float = 0.125, area: list = [105, 125, 15, 28], ):
    lons = np.arange(area[0], area[1]+resolution, resolution)
    lats = np.arange(area[1], area[2]+resolution, resolution) 
    newDa = dataArray.interp(lon=lons, lat=lats, method="linear")
    return newDa

def readFromTDS(initTime: str = '2022031700', modelId: str = 'ecmwfthin', area: list = [105, 125, 15, 28]) -> dict:
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

def main(time):
    if(not time):
        initTime = calLatestBaseTime()  # '2022031500'
    else:
        initTime = time
    arrow_initTime = arrow.get(initTime, 'YYYYMMDDHH')
    try:
        selected_ds = readFromTDS(initTime=initTime)
    except Exception as e:
        return e
    ds_td = selected_ds['td']
    ds_t2m = selected_ds['t2m']
    ds_sst = selected_ds['sst']

    ds_u100 = selected_ds['u100'] 
    ds_v100 = selected_ds['v100']
    ds_u10m = selected_ds['u10m']
    ds_v10m = selected_ds['v10m']
    ds_rhum = selected_ds['rhum']
    ds_temp = selected_ds['temp']
    infoList = []
    isLogExists = False
    ### 读取日志文件 ###
    logInfo = loadLog(arrow_initTime)
    if(logInfo['success']):
        logInfo = json.loads(logInfo['data'])
        isLogExists = True

    for iTimeStep in timeStrList:
        # 读取JSON中的数据, 有成功匹配的值则跳过
        if(isLogExists):  # 如果日志存在并且上次已经成功则跳过此项
            log_item = findLogDataBytimeStep(logInfo, iTimeStep)
            if(log_item and log_item['success']):
                infoList.append(log_item)
                continue

        tdVarName = f't2md{iTimeStep}'
        t2mVarName = f't2mm{iTimeStep}'
        sstVarName = f'sstk{iTimeStep}'

        u100VarName = f'u100{iTimeStep}'
        v100VarName = f'v100{iTimeStep}'
        u10mVarName = f'u10m{iTimeStep}'
        v10mVarName = f'v10m{iTimeStep}'
        rhumVarName = f'rhum{iTimeStep}'
        tempVarName = f'temp{iTimeStep}'


        td = ds_td[tdVarName]
        t2m = ds_t2m[t2mVarName]
        sst = ds_sst[sstVarName]

        u100 = ds_u100[u100VarName]
        v100 = ds_v100[v100VarName]
        u10m = ds_u10m[u10mVarName]
        v10m = ds_v10m[v10mVarName]
        rhum = ds_rhum[rhumVarName]
        temp = ds_temp[tempVarName]
        if(td.min() < -999.0 or t2m.min() < -999.0 or sst.min() < -999.0 or
           u100.min() < -999.0 or v100.min()<-999.0 or u10m.min() < -999.0 or v10m.min() < -999.0 or 
           rhum.min() < -999.0 or temp.min()< -999.0):# TODO 需要添加判断其他要素
            outputInfo = {
                'status': 'error',
                'success': False,
                'initTime': initTime,
                'message': 'No Data -999.9',
                'timeStep': iTimeStep,
            }
            print(outputInfo)
            infoList.append(outputInfo)
            continue
        else:
            td = xr.where(td > 50, td, -999.9)
            td = convertDataArray(td, config['t2md'])
            t2m = xr.where(t2m > 50, t2m, -999.9)
            t2m = convertDataArray(t2m, config['t2mm'])
            sst = xr.where(sst > 50, sst, -999.9)
            sst = convertDataArray(sst, config['sstk'])
            u100 = convertDataArray(u100, config['u100'])
            v100 = convertDataArray(v100, config['v100'])
            u10m = convertDataArray(u10m, config['u10m'])
            v10m = convertDataArray(v10m, config['v10m'])
            rhum = convertDataArray(rhum, config['rhum'])
            temp = convertDataArray(temp, config['temp'])
            # TODO
            t2m_td = t2m - td  # 温度露点差
            td_sst = td - sst  # 露点海温差
            t2m_td = t2m - td  # 温度露点差
            td_sst = td - sst  # 露点海温差
            fog = xr.where(np.logical_and(
                t2m_td >= 0.8, td_sst > 0), -0.5, td_sst)
            fog = convertDataArray(fog, config['fog'])
            ds = xr.Dataset()
            ds['fog'] = fog
            ds['td_sst'] = td_sst
            ds['t2m_td'] = t2m_td
            ds['sst'] = sst
            ds['td'] = td
            ds['t2m'] = t2m

            ####### 储存文件 #######
            fileDir = os.path.join(current_dir, f'../{dataBaseDir}{arrow_initTime.format("YYYY/MM/DDHH/")}')
            fileDir = os.path.normpath(fileDir)
            try:
                drawTools.createDir(fileDir)
                filePath = os.path.join(
                    fileDir, f'fog_{initTime}_{iTimeStep}.nc')
                filePath = os.path.normpath(filePath)
                ds.to_netcdf(filePath)
                print('储存netCDF: '+filePath)
            except Exception as e:
                print(e)

            #####################
            try:
                imgPath = drawTools.drawFogMap(
                    ds, initTime, iTimeStep, imgBaseDir)
                outputInfo = {
                    'status': 'success',
                    'success': True,
                    'initTime': initTime,
                    'message': f'complete {imgPath}',
                    'timeStep': iTimeStep,
                }
                infoList.append(outputInfo)
            except Exception as e:
                outputInfo = {
                    'status': 'error',
                    'success': False,
                    'initTime': initTime,
                    'message': 'Draw Figure Error',
                    'timeStep': iTimeStep,
                }
                infoList.append(outputInfo)
            ds.close()