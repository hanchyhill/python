import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.colors as mcolors
plt.rcParams['font.sans-serif']=['SimHei']
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

import cartopy.crs as ccrs
import cartopy.io.shapereader as shpreader
from cartopy.mpl.ticker import LongitudeFormatter,LatitudeFormatter
# import matplotlib.ticker as mticker

import numpy as np
import xarray as xr
import pandas as pd
import os
import arrow

mpl.use("cairo")

# 相对湿度色标
colorArr_rhum = [
    [0.541176471, 0.31372549, 0.078431373],
    [0.611764706, 0.380392157, 0.121568627],
    [0.674509804, 0.439215686, 0.160784314],
    [0.745098039, 0.505882353, 0.207843137],
    [0.784313725, 0.584313725, 0.298039216],
    [0.82745098, 0.674509804, 0.403921569],
    [0.866666667, 0.756862745, 0.498039216],
    [0.901960784, 0.811764706, 0.592156863],
    [0.933333333, 0.858823529, 0.674509804],
    [0.964705882, 0.909803922, 0.77254902],
    [0.964705882, 0.925490196, 0.831372549],
    [0.960784314, 0.945098039, 0.894117647],
    [0.956862745, 0.960784314, 0.960784314],
    [0.901960784, 0.960784314, 0.819607843],
    [0.780392157, 0.909803922, 0.635294118],
    [0.647058824, 0.835294118, 0.454901961],
    [0.501960784, 0.737254902, 0.28627451],
    [0.37254902, 0.62745098, 0.203921569],
    [0.254901961, 0.51372549, 0.149019608],
    [0.101960784, 0.207843137, 0.070588235],
]
colorArr_rhum.reverse()
clrmap_rhum = mcolors.ListedColormap(colorArr_rhum)

ds = xr.open_dataset("H:/ecmwf/202301/era5-single-202301.nc")
sst = ds['sst']
u10 = ds['u10']
v10 = ds['v10']
t2m = ds['t2m']
d2m = ds['d2m']
time_range=pd.period_range(start='2023-01-11 00:00', end='2023-01-15 12:00', freq='6H').to_timestamp()
shapefiles_dir = 'H:/github/python/seafog/data/shapefiles/'

def draw_sst(time_selected):
    '''
    绘制海温+风场图
    '''
    sst_sd = sst.sel(time=time_selected)
    u10_sd = u10.sel(time=time_selected)
    v10_sd = v10.sel(time=time_selected)
    t2m_sd = t2m.sel(time=time_selected)
    d2m_sd = d2m.sel(time=time_selected)

    plt.clf()
    lon=sst.coords['longitude'][:]#读取经度
    lat=sst.coords['latitude'][:]#读取纬度
    # lons,lats=np.meshgrid(lon,lat)#网格化
    figure = plt.figure(figsize=(16, 9), dpi=120)  # 加载画布
    map_fig = plt.axes(projection=ccrs.PlateCarree())  # 设置投影方式
    shpFilePath01  = os.path.join(shapefiles_dir, './natural_earth/physical/ne_50m_coastline.shp')
    coastmap = shpreader.Reader(shpFilePath01).geometries()  # 读取地图数据

    map_fig.add_geometries(coastmap, ccrs.PlateCarree(),
                          facecolor='none', edgecolor='black')  # 设置边界样式

    shpFilePath02  = os.path.join(shapefiles_dir, './china_basic_map/bou2_4l.shp')
    chinamap = shpreader.Reader(shpFilePath02).geometries()  # 读取地图数据
    map_fig.set_extent([105, 120, 15, 25], crs=ccrs.PlateCarree())  # 设置绘图范围
    map_fig.add_geometries(chinamap, ccrs.PlateCarree(),
                              facecolor='none', edgecolor='black')  # 设置边界样式


    map_fig.set_xticks([105, 110, 115, 120])  # 需要显示的经度，一般可用np.arange
    map_fig.set_yticks([15, 17.5, 20, 22.5, 25])  # 需要显示的纬度
    map_fig.xaxis.set_major_formatter(LongitudeFormatter())  # 将横坐标转换为经度格式
    map_fig.yaxis.set_major_formatter(LatitudeFormatter())  # 将纵坐标转换为纬度格式
    map_fig.tick_params(axis='both', labelsize=15, direction='in',
                           length=5, width=0.55, right=True, top=True)  # 修改刻度样式
    map_fig.grid(linewidth=0.4, color='k', alpha=0.45, linestyle='--')  # 开启网格线

    # 填色图
    range_sst_cf = np.arange(13, 27, 0.5)
    sst_cf = map_fig.contourf(lon, lat, sst_sd-273.15, levels=range_sst_cf, cmap='rainbow',extend='both')
    step = 2
    uv_barb = map_fig.barbs(lon[::step], lat[::step], 
            u10_sd[::step,::step],v10_sd[::step,::step],barb_increments={'half':2,'full':4,'flag':20},
            length=6,sizes={'emptybarb':0}
            )
    # 色标
    cb = figure.colorbar(sst_cf, extend='max', shrink=0.8, pad=0.01)
    cb.set_label('海温', fontdict={'size': 12})
    cb.ax.tick_params(which='major', direction='in', length=6, labelsize=15)

    # 设置文字标题 Set the titles and axes labels
    utcTime = arrow.get(time_selected)
    bjTime = utcTime.shift(hours=8)
    map_fig.set_title(f'10米风, 海温, {bjTime.format("YYYY-MM-DD HH:00")}(北京时)', fontsize=20)

    # 保存图像
    imgDir = os.path.join('H:/github/python/seafog/img/2023/', f'./01/')
    imgDir = os.path.normpath(imgDir)
    imgPath = os.path.join(imgDir, f'sst_{bjTime.format("YYYYMMDDHH")}.jpg')
    plt.savefig(imgPath, format='jpg', bbox_inches='tight', transparent=True)
    print('保存图片: ' + imgPath)
    plt.close(figure)
    

def draw_sst_small(time_selected):
    '''
    绘制海温+风场图
    '''
    sst_sd = sst.sel(time=time_selected)
    u10_sd = u10.sel(time=time_selected)
    v10_sd = v10.sel(time=time_selected)
    t2m_sd = t2m.sel(time=time_selected)
    d2m_sd = d2m.sel(time=time_selected)

    plt.clf()
    lon=sst.coords['longitude'][:]#读取经度
    lat=sst.coords['latitude'][:]#读取纬度
    # lons,lats=np.meshgrid(lon,lat)#网格化
    figure = plt.figure(figsize=(16, 9), dpi=120)  # 加载画布
    map_fig = plt.axes(projection=ccrs.PlateCarree())  # 设置投影方式
    shpFilePath01  = os.path.join(shapefiles_dir, './natural_earth/physical/ne_50m_coastline.shp')
    coastmap = shpreader.Reader(shpFilePath01).geometries()  # 读取地图数据

    map_fig.add_geometries(coastmap, ccrs.PlateCarree(),
                          facecolor='none', edgecolor='black')  # 设置边界样式

    shpFilePath02  = os.path.join(shapefiles_dir, './china_basic_map/bou2_4l.shp')
    chinamap = shpreader.Reader(shpFilePath02).geometries()  # 读取地图数据
    map_fig.set_extent([107, 112, 17.5, 22.5], crs=ccrs.PlateCarree())  # 设置绘图范围
    map_fig.add_geometries(chinamap, ccrs.PlateCarree(),
                          facecolor='none', edgecolor='black')  # 设置边界样式
    map_fig.set_xticks([108, 109, 110,111,112])  # 需要显示的经度，一般可用np.arange
    map_fig.set_yticks([17.5, 18.5, 19.5, 20.5,21.5])  # 需要显示的纬度np.arange
    map_fig.xaxis.set_major_formatter(LongitudeFormatter())  # 将横坐标转换为经度格式
    map_fig.yaxis.set_major_formatter(LatitudeFormatter())  # 将纵坐标转换为纬度格式
    map_fig.tick_params(axis='both', labelsize=15, direction='in',
                           length=5, width=0.55, right=True, top=True)  # 修改刻度样式
    map_fig.grid(linewidth=0.4, color='k', alpha=0.45, linestyle='--')  # 开启网格线

    # 填色图
    range_sst_cf = np.arange(16.5, 25, 0.5)
    sst_cf = map_fig.contourf(lon, lat, sst_sd-273.15, levels=range_sst_cf, cmap='rainbow',extend='both')
    step = 1
    uv_barb = map_fig.barbs(lon[::step], lat[::step], 
            u10_sd[::step,::step],v10_sd[::step,::step],barb_increments={'half':2,'full':4,'flag':20},
            length=6,sizes={'emptybarb':0}
            )
    # 色标
    cb = figure.colorbar(sst_cf, extend='max', shrink=0.8, pad=0.01)
    cb.set_label('海温', fontdict={'size': 12})
    cb.ax.tick_params(which='major', direction='in', length=6, labelsize=15)

    # 设置文字标题 Set the titles and axes labels
    utcTime = arrow.get(time_selected)
    bjTime = utcTime.shift(hours=8)
    map_fig.set_title(f'10米风, 海温, {bjTime.format("YYYY-MM-DD HH:00")}(北京时)', fontsize=20)

    # 保存图像
    imgDir = os.path.join('H:/github/python/seafog/img/2023/', f'./01/')
    imgDir = os.path.normpath(imgDir)
    imgPath = os.path.join(imgDir, f'sst_small_{bjTime.format("YYYYMMDDHH")}.jpg')
    plt.savefig(imgPath, format='jpg', bbox_inches='tight', transparent=True)
    print('保存图片: ' + imgPath)
    plt.close(figure)
    
def draw_layer(var_contourf, u10_sd, v10_sd, time_selected, range_cf, 
    title_pre='10米风, 2m气温, 琼州海峡', cbar_name='气温', file_pre='t2m_small',cmap='rainbow', map_region='QZHX'):
    
    plt.clf()
    lon=sst.coords['longitude'][:]#读取经度
    lat=sst.coords['latitude'][:]#读取纬度
    # lons,lats=np.meshgrid(lon,lat)#网格化
    figure = plt.figure(figsize=(16, 9), dpi=120)  # 加载画布
    map_fig = plt.axes(projection=ccrs.PlateCarree())  # 设置投影方式
    shpFilePath01  = os.path.join(shapefiles_dir, './natural_earth/physical/ne_50m_coastline.shp')
    coastmap = shpreader.Reader(shpFilePath01).geometries()  # 读取地图数据

    map_fig.add_geometries(coastmap, ccrs.PlateCarree(),
                          facecolor='none', edgecolor='black')  # 设置边界样式

    shpFilePath02  = os.path.join(shapefiles_dir, './china_basic_map/bou2_4l.shp')
    chinamap = shpreader.Reader(shpFilePath02).geometries()  # 读取地图数据
    if map_region == 'QZHX': # 琼州海峡
        map_fig.set_extent([108, 112.5, 17.5, 22.5], crs=ccrs.PlateCarree())  # 设置绘图范围
        map_fig.set_xticks([108, 109, 110,111,112])  # 需要显示的经度，一般可用np.arange
        map_fig.set_yticks([17.5, 18.5, 19.5, 20.5,21.5])  # 需要显示的纬度
    elif map_region == 'HNYH': # 华南沿海
        map_fig.set_extent([105, 120, 15, 25], crs=ccrs.PlateCarree())  # 设置绘图范围
        map_fig.set_xticks([105, 110, 115, 120])  # 需要显示的经度，一般可用np.arange
        map_fig.set_yticks([15, 17.5, 20, 22.5, 25])  # 需要显示的纬度
    else:
        raise TypeError(f'错误的地图范围, map_region->{map_region}')

    map_fig.xaxis.set_major_formatter(LongitudeFormatter())  # 将横坐标转换为经度格式
    map_fig.yaxis.set_major_formatter(LatitudeFormatter())  # 将纵坐标转换为纬度格式
    map_fig.add_geometries(chinamap, ccrs.PlateCarree(),
                              facecolor='none', edgecolor='black')  # 设置边界样式
    map_fig.tick_params(axis='both', labelsize=15, direction='in',
                           length=5, width=0.55, right=True, top=True)  # 修改刻度样式
    map_fig.grid(linewidth=0.4, color='k', alpha=0.45, linestyle='--')  # 开启网格线

    # 填色图
    # range_cf = np.arange(14, 25, 0.5)
    # sst_cf = map_fig.contourf(lon, lat, sst_sd-273.15, levels=range_sst_cf, cmap='rainbow',extend='both')
    cf = map_fig.contourf(lon, lat, var_contourf, levels=range_cf,cmap=cmap,extend='both')
    if map_region == 'QZHX': # 琼州海峡
        step = 1
    else:
        step = 2
    uv_barb = map_fig.barbs(lon[::step], lat[::step], 
            u10_sd[::step,::step],v10_sd[::step,::step],barb_increments={'half':2,'full':4,'flag':20},
            length=6,sizes={'emptybarb':0.01}
            )
    # 色标
    cb = figure.colorbar(cf, extend='max', shrink=0.8, pad=0.01)
    cb.set_label(cbar_name, fontdict={'size': 12})
    cb.ax.tick_params(which='major', direction='in', length=6, labelsize=15)

    # 设置文字标题 Set the titles and axes labels
    utcTime = arrow.get(time_selected)
    bjTime = utcTime.shift(hours=8)
    map_fig.set_title(f'{title_pre} {bjTime.format("YYYY-MM-DD HH:00")}(北京时)', fontsize=20)

    # 保存图像
    imgDir = os.path.join('H:/github/python/seafog/img/2023/', f'./01/')
    imgDir = os.path.normpath(imgDir)
    imgPath = os.path.join(imgDir, f'{file_pre}_{bjTime.format("YYYYMMDDHH")}.jpg')
    plt.savefig(imgPath, format='jpg', bbox_inches='tight', transparent=True)
    print('保存图片: ' + imgPath)
    plt.close(figure)


def loop():
    time_range=pd.period_range(start='2023-01-11 00:00', end='2023-01-15 12:00', freq='3H').to_timestamp()
    range_sst_cf = np.arange(16.5, 25, 0.5)
    range_t2m_cf = np.arange(14, 25, 0.5)
    range_t2m_minus_sst_cf = np.arange(-4.0, 4, 0.5)
    range_t2m_minus_d2m_cf = np.arange(0, 5, 0.2)

    for i_time in time_range:
        # draw_sst(i_time)
        # draw_sst_small(i_time, range_sst_cf)
        sst_sd = sst.sel(time=i_time)
        u10_sd = u10.sel(time=i_time)
        v10_sd = v10.sel(time=i_time)
        t2m_sd = t2m.sel(time=i_time)
        d2m_sd = d2m.sel(time=i_time)
        # draw_layer( t2m_sd-sst_sd, u10_sd, v10_sd, 
        #             i_time, range_t2m_minus_sst_cf, 
        #             title_pre='10米风, 气海温差 t2m - sst', cbar_name='℃', file_pre='t2m_minus_sst_', cmap='seismic', map_region='HNYH')
        
        # draw_layer( t2m_sd-sst_sd, u10_sd, v10_sd, 
        #             i_time, range_t2m_minus_sst_cf, 
        #             title_pre='10米风, t2m - sst, 琼州海峡', cbar_name='℃', file_pre='t2m_minus_sst_small', cmap='seismic')
        
        # draw_layer( d2m_sd-sst_sd, u10_sd, v10_sd, 
        #             i_time, range_t2m_minus_sst_cf, 
        #             title_pre='10米风, 露点海温差, 琼州海峡', cbar_name='℃', file_pre='d2m_minus_sst_small', 
        #             cmap='seismic', map_region='QZHX')
        
        # draw_layer( d2m_sd-sst_sd, u10_sd, v10_sd, 
        #             i_time, range_t2m_minus_sst_cf, 
        #             title_pre='10米风, 露点海温差, ', cbar_name='℃', file_pre='d2m_sst_', 
        #             cmap='seismic', map_region='HNYH')
        
        # draw_layer( t2m_sd - d2m_sd, u10_sd, v10_sd, 
        #             i_time, range_t2m_minus_d2m_cf, 
        #             title_pre='10米风, 温度露点差, ', cbar_name='℃', file_pre='t2m_d2m_', 
        #             cmap=clrmap_rhum, map_region='HNYH')
        
        # draw_layer(t2m_sd-273.15, u10_sd, v10_sd, i_time, range_t2m_cf, title_pre='10米风, 2m气温, 琼州海峡', cbar_name='气温', file_pre='t2m_small')
        
        # draw_layer(t2m_sd-273.15, u10_sd, v10_sd, 
        #             i_time, range_t2m_cf, 
        #             title_pre='10米风, 2m气温', cbar_name='气温', file_pre='t2m_', cmap='rainbow', map_region='HNYH')

        draw_layer(d2m_sd-273.15, u10_sd, v10_sd, 
                    i_time, range_t2m_cf, 
                    title_pre='10米风, 2m露点温度', cbar_name='温度', file_pre='d2m', cmap='rainbow', map_region='HNYH')
loop()
