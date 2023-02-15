import xarray as xr
import numpy as np
import pandas as pd

import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.colors as mcolors
plt.rcParams['font.sans-serif']=['SimHei']
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

import cartopy.crs as ccrs
import cartopy.io.shapereader as shpreader
from cartopy.mpl.ticker import LongitudeFormatter,LatitudeFormatter
import arrow
import os

# 读取netCDF文件
ds = xr.open_dataset("H:/ecmwf/202301/era5-single-202301.nc")


def get_ds_slice(time_end):
    time_start = time_end - pd.Timedelta(24,unit='h')
    ds_slice = ds.sel(time=slice(time_start, time_end))
    return ds_slice

def getBackTrack(target_lat, target_lon,ds_slice):
    # 指定目标经纬度
    lat_idx = target_lat
    lon_idx = target_lon

    latlon_list = []
    # 反向追踪气团
    for t in range(len(ds_slice['time'])-1, -1, -1):

        # 计算气团的位移量
        # print(t, lat_idx, lon_idx)
        u_interp = ds_slice['u10'].interp(time=ds_slice['time'][t], latitude=lat_idx, longitude=lon_idx).item()
        v_interp =ds_slice['v10'].interp(time=ds_slice['time'][t], latitude=lat_idx, longitude=lon_idx).item()
        # print(f'u={u_interp.item()}, v={v_interp.item()}')
        du = u_interp * 3600  # 单位：米/小时
        dv = v_interp * 3600  # 单位：米/小时
        latlon_list.append([lat_idx, lon_idx,u_interp,v_interp,du,dv])

        # 计算新的经纬度
        new_lat = lat_idx - dv / 111000  # 单位：度
        new_lon = lon_idx - du / (111000 * np.cos(np.deg2rad(lat_idx)))  # 单位：度

        # 更新索引
        lat_idx = new_lat
        lon_idx = new_lon

        # 输出结果
        # print(f't={t}, lat={lat_idx}, lon={lon_idx}')
    return latlon_list

def plot_track(track_group,ds_slice):
    # Create a new plot
    plt.clf()
    figure = plt.figure(figsize=(16, 9), dpi=120)  # 加载画布
    map_fig = plt.axes(projection=ccrs.PlateCarree())  # 设置投影方式
    map_fig.set_extent([105, 114, 15, 22], crs=ccrs.PlateCarree())  # 设置绘图范围
    map_fig.set_xticks([106, 108,110,112, 114])  # 需要显示的经度，一般可用np.arange
    map_fig.set_yticks([15, 17, 19, 21])  # 需要显示的纬度
    map_fig.xaxis.set_major_formatter(LongitudeFormatter())  # 将横坐标转换为经度格式
    map_fig.yaxis.set_major_formatter(LatitudeFormatter())  # 将纵坐标转换为纬度格式
    map_fig.tick_params(axis='both', labelsize=15, direction='in',
                           length=5, width=0.55, right=True, top=True)  # 修改刻度样式
    map_fig.grid(linewidth=0.4, color='k', alpha=0.45, linestyle='--')  # 开启网格线
    # Plot the data as a line
    for i_track in track_group:
        track_data = np.array(i_track)
        track_lats = track_data[:, 0]
        track_lons = track_data[:, 1]
        map_fig.plot(track_lons, track_lats,color='r')
    # map_fig.barbs(track_lons, track_lats,track_u,track_v,
    #                 barb_increments={'half':2,'full':4,'flag':20},
    #                 length=6,sizes={'emptybarb':0})
    # Add some map features
    map_fig.coastlines()
    start = pd.to_datetime(ds_slice['time'][0].values)
    end = pd.to_datetime(ds_slice['time'][-1].values)
    start_bj = arrow.get(start).shift(hours=8)
    end_bj = arrow.get(end).shift(hours=8)
    map_fig.set_title(f'琼州海峡后向轨迹 {start_bj.format("MM-DD HH:00")} 至{end_bj.format("DD日HH:00")}', fontsize=20)
    # 保存图像
    imgDir = os.path.join('H:/github/python/seafog/img/2023/', f'./01/')
    imgDir = os.path.normpath(imgDir)
    imgPath = os.path.join(imgDir, f'backtrack_{start_bj.format("YYYYMMDDHH")}_{end_bj.format("YYYYMMDDHH")}.jpg')
    plt.savefig(imgPath, format='jpg', bbox_inches='tight', transparent=True)
    print('保存图片: ' + imgPath)
    plt.close(figure)
    # Show the plot
    # plt.show()

def main(time_end):
    ds_slice =get_ds_slice(time_end)
    target_lats, target_lons = np.meshgrid(np.arange(19.97,21.0,0.2), np.arange(109.96,111.13,0.2))
    target_lats = target_lats.reshape(-1)
    target_lons = target_lons.reshape(-1)
    track_group = []
    for index in range(len(target_lats)):
        target_lat=target_lats[index]
        target_lon=target_lons[index]
        i_track = getBackTrack(target_lat, target_lon,ds_slice)
        track_group.append(i_track)
    plot_track(track_group, ds_slice)

def loop():
    time_end = pd.to_datetime('2023-01-15 02:00')  + pd.Timedelta(-8,unit='h')
    main(time_end)
    time_end = pd.to_datetime('2023-01-14 02:00')  + pd.Timedelta(-8,unit='h')
    main(time_end)
    time_end = pd.to_datetime('2023-01-13 02:00')  + pd.Timedelta(-8,unit='h')
    main(time_end)

loop()