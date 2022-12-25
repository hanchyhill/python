import xarray as xr
import numpy as np
import libs.drawMap as drawTools
import libs.utils as util
import os
import arrow
import json
import time
import pandas as pd

import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

timeStrList = ['000', '003', '006', '009', '012', '015', '018', '021', '024', '027', '030', '033', '036', '039', '042', '045', '048', '051', '054', '057', '060', '063', '066', '069', '072', '078', '084', '090',
               '096', '102', '108', '114', '120', '126', '132', '138', '144', '150', '156', '162', '168', '174', '180', '186', '192', '198', '204', '210', '216', '222', '228', '234', '240', ]
fcHour_list = list(range(0, 72+1, 3)) + list(range(78, 240+1, 6))

config = util.nwp_var_config

current_dir = os.path.dirname(__file__)
if os.name == 'nt':
    if os.environ['COMPUTERNAME'] == 'DESKTOP-EQAO3M5':
      computer_flag = 'home'
    elif  os.environ['COMPUTERNAME'] == 'H1809-P014':
      computer_flag = 'office'
    else:
      computer_flag = 'office2'
else:
    computer_flag = 'server'

if computer_flag == 'home':
    root_dir = "F:/github/pythonScript/seafog/"
    imgBaseDir = 'F:/github/python/seafog/img/'
    dataBaseDir = 'F:/github/python/seafog/img/online/'
elif  computer_flag == 'office':
    root_dir = "H:/github/python/seafog/"
    imgBaseDir = 'H:/github/python/seafog/img/'
    dataBaseDir = 'H:/github/python/seafog/img/online/'
else:
    root_dir = "/var/www/html/seafog-DL/"
    imgBaseDir = '/var/www/html/data/img/seafog_DL/'
    dataBaseDir = '/var/www/html/data/store/seafog_DL/'

def loadLog(baseDir, time, type='map'):
    if(type=='map'):
        fileName = f'log_{time.format("YYYYMMDDHH")}.json'
    elif(type=='point'):
        fileName = f'log_timeSeries_{time.format("YYYYMMDDHH")}.json'
    else:
        print('请设置正确的日志type: ' + type)
        raise '请设置正确的日志type: ' + type
    
    logDir = os.path.join(
        baseDir, f'./{time.format("YYYY/MM/DDHH/")}')
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

def saveLog(baseDir, ctx, time, type='map'):
    '''
    写入日志
    params:baseDir:基部路径
    params:ctx:需要写入的内容
    params:time:时间变量
    params:type:enum(map,point),类型
    '''
    logDir = os.path.join(
        baseDir, f'./{time.format("YYYY/MM/DDHH/")}')
    logDir = os.path.normpath(logDir)
    if(type=='map'):
        fileName = f'log_{time.format("YYYYMMDDHH")}.json'
    elif(type=='point'):
        fileName = f'log_timeSeries_{time.format("YYYYMMDDHH")}.json'
    else:
        print('请设置正确的日志type: ' + type)
        raise '请设置正确的日志type: ' + type
    try:
        drawTools.createDir(logDir)
        logPath = os.path.join(logDir, fileName)
        print('写入日志路径'+logPath)
        with open(logPath, 'w') as f_log:
            f_log.write(ctx)
    except Exception as e:
        raise e

findLogDataBytimeStep = util.findLogDataBytimeStep
findLogDataByStationId = util.findLogDataByStationId

convertDataArray = util.convertDataArray

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

readFromTDS = util.readFromTDS_seafog

def main(time=None):
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
    logInfo = loadLog(imgBaseDir, arrow_initTime)
    if(logInfo['success']):
        print('读取日志成功')
        logInfo = json.loads(logInfo['data'])
        isLogExists = True
    else:
        print('未找到当前时次日志')
    for iHour in fcHour_list:
        iTimeStep = f'{iHour:0>3d}'
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
            td = xr.where(td > 50, td, np.nan)
            td = convertDataArray(td, config['t2md'])
            t2m = xr.where(t2m > 50, t2m, np.nan)
            t2m = convertDataArray(t2m, config['t2mm'])
            sst = xr.where(sst > 273.161, sst, np.nan)
            sst = convertDataArray(sst, config['sstk'])
            u100 = convertDataArray(u100, config['u100'])
            v100 = convertDataArray(v100, config['v100'])
            u10m = convertDataArray(u10m, config['u10m'])
            v10m = convertDataArray(v10m, config['v10m'])
            rhum = convertDataArray(rhum, config['rhum'])
            temp = convertDataArray(temp, config['temp'])
            # TODO
            # t2m_td = t2m - td  # 温度露点差
            # td_sst = td - sst  # 露点海温差
            # t2m_td = t2m - td  # 温度露点差
            # td_sst = td - sst  # 露点海温差
            dalist = [td, t2m, sst, u100, v100, u10m, v10m, rhum, temp]
            fog = drawTools.predictFog(dalist, iHour)
            fog = convertDataArray(fog, config['fog'])
            ds = xr.Dataset()
            ds['fog'] = fog
            ds['u100'] = u100
            ds['v100'] = v100
            ds['sstk'] = sst
            ds['t2md'] = td
            ds['t2mm'] = t2m
            ds['rhum'] = rhum
            ds['temp'] = temp
            ds['u10m'] = u10m
            ds['v10m'] = v10m

            ####### 储存文件 #######
            # fileDir = os.path.join(dataBaseDir, f'./{arrow_initTime.format("YYYY/MM/DDHH/")}')
            # fileDir = os.path.normpath(fileDir)
            # try:
            #     drawTools.createDir(fileDir)
            #     filePath = os.path.join(
            #         fileDir, f'fog_{initTime}_{iTimeStep}.nc')
            #     filePath = os.path.normpath(filePath)
            #     ds.to_netcdf(filePath)
            #     print('储存netCDF: '+filePath)
            # except Exception as e:
            #     print(e)

            #####################
            try:
                imgPath = drawTools.draw_Fog_Contour(
                    fog, initTime, iTimeStep, imgBaseDir)
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
    
    ds_td.close()
    ds_t2m.close()
    ds_sst.close()
    ds_u100.close()
    ds_v100.close()
    ds_u10m.close()
    ds_v10m.close()
    ds_rhum.close()
    ds_temp.close()
    #############写入日志###########
    logOutput = json.dumps(infoList, indent=2)
    saveLog(imgBaseDir, logOutput, arrow_initTime, type='map')

##########自定义任务##########
def customTask():
    dateList = pd.date_range(start='2022-11-01 00:00', end='2022-11-28 12:00',freq='12H')
    for iDate in dateList:
        initTime = arrow.get(iDate).format('YYYYMMDDHH')
        main(initTime)

# customTask()

##########创建任务##########
def testTime():
    startTime = time.process_time()
    main()
    endTime = time.process_time()
    print(f'总时间为{str((endTime-startTime)/60.0)}分钟')
# demoTest()

job_defaults = {
    'coalesce': True,  # 合并执行misfire的任务, 减少执行次数
    'max_instances': 1  # 只有一个任务进程, 上个任务未执行完毕将取消执行本次运行
}


def my_listener(event):
    if event.exception:
        print('任务出错了！！！！！！')
    else:
        print('任务照常运行...')


scheduler = BlockingScheduler(job_defaults=job_defaults)  # 创建后台非阻塞任务
scheduler.add_job(testTime, 'interval', minutes=12, id='draw_fog_map', next_run_time=datetime.datetime.now())
scheduler.add_listener(my_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

try:
    scheduler.start()
except (KeyboardInterrupt, SystemExit):
    pass

print('正在执行海上能见度定时任务')