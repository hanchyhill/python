import xarray as xr
import arrow

fcHr = [0, 3, 6, 9, 12, 15, 18, 21, 24, 27, 30, 33, 36,
        39, 42, 45, 48, 51, 54, 57, 60, 63, 66, 69, 72, 78, 84, 90, 96, 102, 108, 114, 120, 126,
        132, 138, 144, 150, 156, 162, 168, 174, 180, 192, 198, 204, 210, 216, 222, 228, 234, 240]
fixLoc = [110.25, 20.125]
elemName = 'sstk'
eleList = ['visi', 'sstk', 't2mm', 't2md']
initTime = arrow.get(2018, 10, 1)
endTime = arrow.get(2021,1,1)

def readFixPointData(year=2018, month=10, elem='visi', fixPoint=[110.25, 20.125]):
    '''
    读取单点数据
    '''
    url = 'http://10.148.8.71:7080/thredds/dodsC/ecmwf_fine_his/{0}{1:0>2d}/{2}.nc'.format(
        year, month, elem)
    dataset = xr.open_dataset(url)
    varName = '{}{:0>3d}'.format(elem, 24)
    # dataArray = dataset[varName]
    pointDs = dataset.loc[{'lat': fixPoint[1], 'lon': fixPoint[0]}]
    return pointDs


def main():
    print(elemName)
    arrow.Arrow.range('month', initTime, endTime)
    baseDs = readFixPointData(initTime.year, initTime.month, elemName, fixLoc)
    for iTime in arrow.Arrow.range('month', initTime.shift(months=+1), endTime):  # 迭代日期, 合并数据
        # iTime = initTime.shift(months=+num)
        iDs = readFixPointData(iTime.year, iTime.month, elemName, fixLoc)
        print('合并中{}'.format(iTime))
        baseDs = xr.concat([baseDs, iDs], dim="time")

    baseDs.to_netcdf("{}-{}_lon{}_lat{}.{}.nc".format(initTime.format('YYYYMM'), endTime.format('YYYYMM'), fixLoc[0], fixLoc[1], elemName))
main()
