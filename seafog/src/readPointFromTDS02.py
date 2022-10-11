import xarray as xr
import arrow
import os
import json
from queue import Queue
from threading import Thread
 
current_file_dir = os.path.dirname(__file__)  # 当前文件所在的目录

stationIDs = ["57988","57989","57996","59071","59072","59074","59075","59081","59082","59087","59088","59090","59094","59096","59097","59099","59106","59107","59109","59114","59116","59117","59264","59268","59269","59270","59271","59276","59278","59279","59280","59284","59285","59287","59288","59289","59290","59293","59294","59297","59298","59303","59304","59306","59310","59312","59313","59314","59315","59316","59317","59318","59319","59324","59456","59462","59469","59470","59471","59473","59475","59476","59477","59478","59480","59481","59485","59487","59488","59492","59493","59500","59501","59502","59650","59653","59654","59655","59656","59658","59659","59663","59664","59750","59754","59673"]
# stationIDs = ["57989","57996","59071","59072","59074","59075","59081","59082","59087","59088","59090","59094","59096","59097","59099","59106","59107","59109","59114","59116","59117","59264","59268","59269","59270","59271","59276","59278","59279","59280","59284","59285","59287","59288","59289","59290","59293","59294","59297","59298","59303","59304","59306","59310","59312","59313","59314","59315","59316","59317","59318","59319","59324","59456","59462","59469","59470","59471","59473","59475","59476","59477","59478","59480","59481","59485","59487","59488","59492","59493","59500","59501","59502","59650","59653","59654","59655","59656","59658","59659","59663","59664","59750","59754","59673"]
fcHr = [0, 3, 6, 9, 12, 15, 18, 21, 24, 27, 30, 33, 36,
        39, 42, 45, 48, 51, 54, 57, 60, 63, 66, 69, 72, 78, 84, 90, 96, 102, 108, 114, 120, 126,
        132, 138, 144, 150, 156, 162, 168, 174, 180, 192, 198, 204, 210, 216, 222, 228, 234, 240]

fixLoc = [110.25, 20.125]
fixLocUpper = [110.25, 20.25]
elemName = 't2mm'
eleList = ['sktk']#['mn2t','mx2t']#['v10m','u10m','u100','v100'] # 'visi', 'sstk', 't2mm', 't2md',
upperEleList = ['uwnd','vwnd'] # 'temp','rhum', 'temp',
modelList = ['ecmwf_fine_his', 'ecmwfthin']
initTime = arrow.get(2018, 10, 1)
endTime = arrow.get(2022, 5, 1)
realTimeEndTime = arrow.get(2021,3,1)

def readFixPointData(model='ecmwf_fine_his', year=2018, month=10, elem='visi', fixPoint=[110.25, 20.125]):
    '''
    读取单点数据
    '''
    url = 'http://10.148.8.71:7080/thredds/dodsC/{0}/{1}{2:0>2d}/{3}.nc'.format(
        model, year, month, elem)
    try:
        dataset = xr.open_dataset(url)
    except Exception as e:
        print('无法获取数据源')
        raise e    
    # varName = '{}{:0>3d}'.format(elem, 24)
    # dataArray = dataset[varName]
    pointDs = dataset.loc[{'lat': fixPoint[1], 'lon': fixPoint[0]}]
    return pointDs

def readFixPointDataFromPressue(model='ecmwf_fine_his', year=2018, month=10, elem='visi', fixPoint=[110.25, 20.25]):
    '''
    读取单点数据,
    '''
    url = 'http://10.148.8.71:7080/thredds/dodsC/{0}/{1}{2:0>2d}/{3}.nc'.format(
        model, year, month, elem)
    dataset = xr.open_dataset(url)
    varName = '{}{:0>3d}'.format(elem, 24)
    # dataArray = dataset[varName]
    pointDs = dataset.loc[{'lat': fixPoint[1], 'lon': fixPoint[0]}]
    return pointDs

def concatData(elemName, lonlat, stationID):
    print(elemName,stationID)
    currenModel = 'ecmwf_fine_his'
    arrow.Arrow.range('month', initTime, endTime)
    baseDs = readFixPointData(currenModel, initTime.year, initTime.month, elemName, lonlat)
    for iTime in arrow.Arrow.range('month', initTime.shift(months=+1), endTime):  # 迭代日期, 合并数据
        try:
            iDs = readFixPointData(currenModel, iTime.year, iTime.month, elemName, lonlat)
            print(f'合并中{iTime}, {elemName}, {stationID}')
            baseDs = xr.concat([baseDs, iDs], dim="time")
        except Exception as e:
            print(f'更换数据源{iTime}, {elemName}, {stationID}')
            try:
                iDs = readFixPointData('ecmwfthin', iTime.year, iTime.month, elemName, lonlat)
                print(f'合并中{iTime}, {elemName}, {stationID}')
                baseDs = xr.concat([baseDs, iDs], dim="time")
            except Exception as e:
                print(f'所有数据源无数据{elemName},站号{stationID}, {iTime.format()}, 跳过此循环')
                continue

    # currenModel = 'ecmwfthin'
    # for iTime in arrow.Arrow.range('month', endTime.shift(months=+1), realTimeEndTime):  # 迭代日期, 合并数据
    #     iDs = readFixPointData(currenModel, iTime.year, iTime.month, elemName, fixLoc)
    #     print('合并中{}'.format(iTime))
    #     baseDs = xr.concat([baseDs, iDs], dim="time")
    
    netcdf_file_name = "{}.{}-{}_lon{}_lat{}.{}.nc".format(stationID, initTime.format('YYYYMM'), endTime.format('YYYYMM'), lonlat[0], lonlat[1], elemName)
    fullPath = os.path.join(current_file_dir, './{}'.format(netcdf_file_name))
    baseDs.to_netcdf(fullPath)
    baseDs.close()

def concatPreData(elemName):
    print(elemName)
    currenModel = 'ecmwf_fine_his'
    arrow.Arrow.range('month', initTime, endTime)
    baseDs = readFixPointDataFromPressue(currenModel, initTime.year, initTime.month, elemName, fixLocUpper)
    for iTime in arrow.Arrow.range('month', initTime.shift(months=+1), endTime):  # 迭代日期, 合并数据
        print(iTime)
        iDs = readFixPointData(currenModel, iTime.year, iTime.month, elemName, fixLocUpper)
        print('合并中{}'.format(iTime))
        baseDs = xr.concat([baseDs, iDs], dim="time")

    # currenModel = 'ecmwfthin'
    # for iTime in arrow.Arrow.range('month', endTime.shift(months=+1), realTimeEndTime):  # 迭代日期, 合并数据
    #     print(iTime)
    #     iDs = readFixPointDataFromPressue(currenModel, iTime.year, iTime.month, elemName, fixLocUpper)
    #     print('合并中{}'.format(iTime))
    #     baseDs = xr.concat([baseDs, iDs], dim="time")
    
    netcdf_file_name = "{}-{}_lon{}_lat{}.{}.nc".format(initTime.format('YYYYMM'), realTimeEndTime.format('YYYYMM'), fixLocUpper[0], fixLocUpper[1], elemName)
    fullPath = os.path.join(current_file_dir, '../data/{}'.format(netcdf_file_name))
    baseDs.to_netcdf(fullPath)

def fix2Grid(number, interval = 0.125):
    fixNum = round(number / interval) * interval
    return fixNum
  

def getStationLoc(ID, stationInfo):
    iStation = stationInfo[ID]
    lonlat  = [fix2Grid(iStation["Longitude"]), fix2Grid(iStation["Latitude"])]
    return lonlat

def main():
    with open(os.path.join(current_file_dir,'./contry_station.guangdong.json'),'r',encoding='utf-8')as f:
        stationInfo = json.load(f)
    arg_arr = []
    for ID in stationIDs:
        lonlat = getStationLoc(ID, stationInfo)
        for elemName in eleList:
            arg_arr.append((elemName, lonlat,ID))
            # concatData(elemName, lonlat,ID)

    #创建一个主进程与工作进程通信
    queue = Queue()
    #创建10个工作线程
    for x in range(10):
        worker = DownloadWorker(queue)
        #将daemon设置为True将会使主线程退出，即使所有worker都阻塞了
        worker.daemon = True
        worker.start()
    #将任务以tuple的形式放入队列中
    for args in arg_arr:
        queue.put(args)
    #让主线程等待队列完成所有的任务
    queue.join()
    # for elemName in upperEleList:
    #     concatPreData(elemName)

#下载脚本 
class DownloadWorker(Thread):
  def __init__(self, queue):
    Thread.__init__(self)
    self.queue = queue

  def run(self):
    while True:
      # 从队列中获取任务并扩展tuple
      elemName, lonlat,ID = self.queue.get()
      concatData(elemName, lonlat,ID)
      self.queue.task_done()

# main()
concatData('tppm',[110.25,20.125],'59754')
