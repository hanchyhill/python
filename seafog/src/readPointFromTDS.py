import xarray as xr
import arrow
import os
 
current_file_dir = os.path.dirname(__file__)  # 当前文件所在的目录

fcHr = [0, 3, 6, 9, 12, 15, 18, 21, 24, 27, 30, 33, 36,
        39, 42, 45, 48, 51, 54, 57, 60, 63, 66, 69, 72, 78, 84, 90, 96, 102, 108, 114, 120, 126,
        132, 138, 144, 150, 156, 162, 168, 174, 180, 192, 198, 204, 210, 216, 222, 228, 234, 240]
fixLoc = [110.25, 20.125]
fixLocUpper = [110.25, 20.25]
elemName = 'sstk'
eleList = ['visi', 'sstk', 't2mm', 't2md']
upperEleList = ['rhum', 'temp']
modelList = ['ecmwf_fine_his', 'ecmwfthin']
initTime = arrow.get(2018, 10, 1)
endTime = arrow.get(2021, 1, 1)
# initTime = arrow.get(2021, 2, 1)
# endTime = arrow.get(2021, 3, 1)
realTimeEndTime = arrow.get(2021,3,1)

def readFixPointData(model='ecmwf_fine_his', year=2018, month=10, elem='visi', fixPoint=[110.25, 20.125]):
    '''
    读取单点数据
    '''
    url = 'http://10.148.8.71:7080/thredds/dodsC/{0}/{1}{2:0>2d}/{3}.nc'.format(
        model, year, month, elem)
    dataset = xr.open_dataset(url)
    varName = '{}{:0>3d}'.format(elem, 24)
    # dataArray = dataset[varName]
    pointDs = dataset.loc[{'lat': fixPoint[1], 'lon': fixPoint[0]}]
    return pointDs

def readFixPointDataFromPressue(model='ecmwf_fine_his', year=2018, month=10, elem='visi', fixPoint=[110.25, 20.25]):
    '''
    读取单点数据,qi
    '''
    url = 'http://10.148.8.71:7080/thredds/dodsC/{0}/{1}{2:0>2d}/{3}.nc'.format(
        model, year, month, elem)
    dataset = xr.open_dataset(url)
    varName = '{}{:0>3d}'.format(elem, 24)
    # dataArray = dataset[varName]
    pointDs = dataset.loc[{'lat': fixPoint[1], 'lon': fixPoint[0]}]
    return pointDs

def concatData(elemName):
    print(elemName)
    currenModel = 'ecmwf_fine_his'
    arrow.Arrow.range('month', initTime, endTime)
    baseDs = readFixPointData(currenModel, initTime.year, initTime.month, elemName, fixLoc)
    for iTime in arrow.Arrow.range('month', initTime.shift(months=+1), endTime):  # 迭代日期, 合并数据
        iDs = readFixPointData(currenModel, iTime.year, iTime.month, elemName, fixLoc)
        print('合并中{}'.format(iTime))
        baseDs = xr.concat([baseDs, iDs], dim="time")

    currenModel = 'ecmwfthin'
    for iTime in arrow.Arrow.range('month', endTime.shift(months=+1), realTimeEndTime):  # 迭代日期, 合并数据
        iDs = readFixPointData(currenModel, iTime.year, iTime.month, elemName, fixLoc)
        print('合并中{}'.format(iTime))
        baseDs = xr.concat([baseDs, iDs], dim="time")
    
    netcdf_file_name = "{}-{}_lon{}_lat{}.{}.nc".format(initTime.format('YYYYMM'), realTimeEndTime.format('YYYYMM'), fixLoc[0], fixLoc[1], elemName)
    fullPath = os.path.join(current_file_dir, '../data/{}'.format(netcdf_file_name))
    baseDs.to_netcdf(fullPath)

def concatPreData(elemName):
    print(elemName)
    currenModel = 'ecmwf_fine_his'
    arrow.Arrow.range('month', initTime, endTime)
    baseDs = readFixPointDataFromPressue(currenModel, initTime.year, initTime.month, elemName, fixLocUpper)
    for iTime in arrow.Arrow.range('month', initTime.shift(months=+1), endTime):  # 迭代日期, 合并数据
        iDs = readFixPointData(currenModel, iTime.year, iTime.month, elemName, fixLocUpper)
        print('合并中{}'.format(iTime))
        baseDs = xr.concat([baseDs, iDs], dim="time")

    currenModel = 'ecmwfthin'
    for iTime in arrow.Arrow.range('month', endTime.shift(months=+1), realTimeEndTime):  # 迭代日期, 合并数据
        iDs = readFixPointDataFromPressue(currenModel, iTime.year, iTime.month, elemName, fixLocUpper)
        print('合并中{}'.format(iTime))
        baseDs = xr.concat([baseDs, iDs], dim="time")
    
    netcdf_file_name = "{}-{}_lon{}_lat{}.{}.nc".format(initTime.format('YYYYMM'), realTimeEndTime.format('YYYYMM'), fixLoc[0], fixLoc[1], elemName)
    fullPath = os.path.join(current_file_dir, '../data/{}'.format(netcdf_file_name))
    baseDs.to_netcdf(fullPath)

def main():
    # for elemName in eleList:
    #     concatData(elemName)
    for elemName in upperEleList:
        concatPreData(elemName)

main()
