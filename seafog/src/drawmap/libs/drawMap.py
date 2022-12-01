import xarray as xr
import numpy as np
import os
os.environ["CUDA_VISIBLE_DEVICES"]="-1"
import arrow
# import json
# import time
import pandas as pd
import datetime
import metpy.calc as mpcalc
from metpy.units import units
import tensorflow as tf
from sklearn.preprocessing import StandardScaler

import matplotlib as mpl
mpl.use("cairo")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.colors as mcolors
plt.rcParams['font.sans-serif']=['SimHei']
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号
import cartopy.crs as ccrs
import cartopy.io.shapereader as shpreader
from cartopy.mpl.ticker import LongitudeFormatter,LatitudeFormatter

timeStrList = ['000', '003', '006', '009', '012', '015', '018', '021', '024', '027', '030', '033', '036', '039', '042', '045', '048', '051', '054', '057', '060', '063', '066', '069', '072', '078', '084', '090',
               '096', '102', '108', '114', '120', '126', '132', '138', '144', '150', '156', '162', '168', '174', '180', '186', '192', '198', '204', '210', '216', '222', '228', '234', '240', ]
fcHour_list = list(range(0, 72+1, 3)) + list(range(78, 240+1, 6))

dir_path = 'H:/data/ec_thin/'
var_symbols = [
't2md',
't2mm',
'sstk',
'u100',
'v100',
'u10m',
'v10m',
'rhum',
'temp',]

ds_list = []
for iSymbol in var_symbols:
    i_dataset = xr.open_dataset(f'http://10.148.8.71:7080/thredds/dodsC/ecmwfthin/202210/{iSymbol}.nc')
    ds_list.append(i_dataset)


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
elif computer_flag == 'office':
    root_dir = "H:/github/python/seafog/"
else:
    root_dir = "/var/www/html/seafog-DL/"

def setDataset(df, x_columns):
    '''
    修改dataFrame
    '''
    df['t_td'] =  df['t2mm'] - df['t2md']
    df['td_sst'] =  df['t2md'] - df['sstk']
    df['t_sst'] =  df['t2mm'] - df['sstk']
    df['delta_theta'] = df['theta925'] - df['theta1000']
    df['delta_theta_e'] = df['theta_e925'] - df['theta_e1000']
    df_x = df.loc[:, x_columns]
    df_y = df['station_vis_linear']
    _x = df_x.to_numpy()
    _y = df_y.to_numpy()
    return (_x, _y)

model = tf.keras.models.load_model(os.path.normpath(os.path.join(root_dir, './data/model/model_singletest_fog_dataset_hdf66_fc120h_v2')))

colordict_fog=['#9C052A','#CC046B','#FF1A3C','#F50068','#00014D','#0003BF','#0004FD','#2629FD','#2280F9','#6EB9F7','#59E144','#BBE7BB','#E6F5E6','#FFFEFF']
clrmap_fog = mcolors.ListedColormap(colordict_fog)
bound_fog = [0, 0.1, 0.2, 0.3, 0.5, 0.8, 1.3, 2.1, 3.4, 5.5]
bound_fog = [0.,50.,200.,500.,1_000.,2_000.,3_000.,4_000.,6_000.,8_000.,10_000.,15_000.,20_000.,25_000.,30_000]
norms_fog = mcolors.BoundaryNorm(bound_fog, clrmap_fog.N)

fog_dataset_hdf = os.path.normpath(os.path.join(root_dir, './data/hdf/fog_dataset_hdf66_fc120h_v2.h5'))
store_dataset = pd.HDFStore(fog_dataset_hdf, mode='r')
df_train = store_dataset.get('train')
x_columns = ['t_td', 'td_sst','t_sst','v100', 'v10m', 'u100', 'u10m', 't2mm', 't2md', 'sstk','year_sin','year_cos', 'day_sin', 'day_cos','delta_theta','delta_theta_e','theta_e925']
(train_x, train_y) = setDataset(df_train, x_columns)
scaler = StandardScaler()
train_x_scaled_fit = scaler.fit(train_x)

config = {
    'sstk':{
        'name':'sstk',
        'missing_value': -999.9,
        # '_FillValue':  -999.9,
        'valid_min': 273.16,
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

def createDir(path:str, silence:bool=True):
    '''
    递归创建文件夹
    :params path: 文件夹路径
    '''
    isExists=os.path.exists(path)
    if not isExists:
        try:
            os.makedirs(path) 
            print (path+' 创建成功')
        except Exception as e:
            print('创建文件夹失败 ' + path)
            raise e
    else:
        if(silence == False):print( '文件夹已存在')
        # print( '文件夹已存在')
        # 如果目录存在则不创建，并提示目录已存在
        # print (path+' 目录已存在')


def interpolate(dataArray,  resolution:float = 0.125 ):
    '''
    空间插值计算
    '''
    lons = np.arange(dataArray.lon[0], dataArray.lon[-1]+resolution, resolution)
    lats = np.arange(dataArray.lat[0], dataArray.lat[-1]+resolution, resolution) 
    newDa = dataArray.interp(lon=lons, lat=lats, method="linear")
    return newDa

def linear_vis(x):
    '''
    能见度标准化
    '''
    if(x <= 1000.0):
      y = x/1000.0
    elif(x > 1000.0 and x <= 10000.0):
      y = (x-1000.0)/9000.0 + 1.0
    elif( x > 10000.0  and x <= 30000.0):
      y = (x-10000.0)/20000 + 2.0
    else:
      y = 3.0
    return y

def reverse_linear_vis(x):
    '''
    标准化能见度转能见度
    '''
    if(np.isnan(x)):
      y = np.nan
    elif(x<0):
      y = 1
    elif(x <= 1.0):
      y = x*1000.0
    elif(x <= 2.0):
      y = (x - 1.0)*9000.0 + 1000.0
    elif(x <= 3.0):
      y = (x-2.0)*20000 + 10000.0
    else:
      y = 30000.0
    return y



# time_step = '024'
# area = [105, 125, 15, 28]
# da_list = []
# for iSymbol, i_ds in zip(var_symbols, ds_list):
#     i_dataArray = i_ds[f'{iSymbol}{time_step}']
#     print(iSymbol)
#     if iSymbol == 'rhum' or iSymbol == 'temp':
#         sud_dataArray = i_dataArray.sel(time=i_dataArray.time[0].values.item(), level=[1000.0, 925.0], lat=slice(area[2], area[3]), lon=slice(area[0], area[1]))
#     else:
#         sud_dataArray = i_dataArray.sel(time=i_dataArray.time[0].values.item(), level=0.0, lat=slice(area[2], area[3]), lon=slice(area[0], area[1]))
#     da_list.append(sud_dataArray)





def predictFog(da_list, leadtime)->xr.DataArray:
    '''
    预报能见度
    '''
    (t2md, t2mm,sstk,u100,v100,u10m,v10m,rhum,temp) = da_list
    # sstk = xr.where(sstk>273.16, sstk, np.nan)

    # 计算时间周期
    iTime = pd.Timestamp(t2md.time.values.item())  + pd.Timedelta(leadtime,unit='h')
    year_sin = np.sin((iTime.dayofyear / 365.25) * 2 * np.pi)
    year_cos = np.cos((iTime.dayofyear / 365.25) * 2 * np.pi)
    day_sin = np.sin((iTime.hour / 24) * 2 * np.pi)
    day_cos = np.cos((iTime.hour / 24) * 2 * np.pi)

    # 计算露点温度
    rhum_unit = np.clip(rhum, 0, 100)*units.percent
    temp_unit = temp*units.kelvin
    td_upper = mpcalc.dewpoint_from_relative_humidity(temp_unit, rhum_unit)

    # 生成经纬度网格，计算位温和相当位温
    _x, level_mesh, _z = np.meshgrid(temp.lat, temp.level,temp.lon)
    level_mesh = np.array(level_mesh)*units.hPa
    theta_e = mpcalc.equivalent_potential_temperature(level_mesh, temp_unit, td_upper)
    theta = mpcalc.potential_temperature(level_mesh, temp_unit)
    theta_e = theta_e.metpy.convert_units('degC')
    theta = theta.metpy.convert_units('degC')

    # 插值高空网格到0.125°
    theta_e_interp = interpolate(theta_e)
    theta_interp = interpolate(theta)


    keep_cols = ['t_td', 'td_sst','t_sst','year_sin','year_cos', 'day_sin', 'day_cos','delta_theta','delta_theta_e']
    x_columns = ['t_td', 'td_sst','t_sst','v100', 'v10m', 'u100', 'u10m', 't2mm', 't2md', 'sstk','year_sin','year_cos', 'day_sin', 'day_cos','delta_theta','delta_theta_e','theta_e925']

    # 计算各因变量
    t_td = t2mm - t2md
    td_sst = t2md - sstk
    t_sst = t2mm - sstk
    delta_theta = theta_interp.sel(level=925.0) - theta_interp.sel(level=1000.0)
    delta_theta_e = theta_e_interp.sel(level=925.0) - theta_e_interp.sel(level=1000.0)
    theta_e925 = theta_e_interp.sel(level=925.0)

    # 转换为摄氏度℃
    t2mm = t2mm.metpy.convert_units('degC')
    t2md = t2md.metpy.convert_units('degC')
    sstk = sstk.metpy.convert_units('degC')


    # 组合各变量为TF model 的 因变量输入
    stack_list = [t_td, td_sst, t_sst, v100, v10m, u100, u10m, t2mm, t2md, sstk,delta_theta,delta_theta_e,theta_e925]
    one_dim_list = []
    for arr in stack_list:
        stacked = arr.stack(z=("lat","lon"))
        one_dim_list.append(stacked.values)

    year_sin_list = np.full_like(one_dim_list[0], year_sin)
    year_cos_list = np.full_like(one_dim_list[0], year_cos)
    day_sin_list = np.full_like(one_dim_list[0], day_sin)
    day_cos_list = np.full_like(one_dim_list[0], day_cos)
    time_list = [year_sin_list, year_cos_list, day_sin_list, day_cos_list]

    full_list = one_dim_list[:10] + time_list + one_dim_list[10:]
    order_arr = np.array(full_list)
    order_x = order_arr.T
    order_x_scale = scaler.transform(order_x)

    for iColumn in keep_cols:
        index = x_columns.index(iColumn)
        order_x_scale[:,index] = order_x[:,index]

    predictions = model(order_x_scale).numpy()
    stack_sample = t_td.stack(z=("lat","lon"))
    vis = np.vectorize(reverse_linear_vis)(predictions[:,0])
    stack_sample.values = vis
    prediction_vis = stack_sample.unstack("z")
    return prediction_vis

def draw_Fog_Contour(dataArray, baseTime:str='2022031700', timeStep:str='000', imgBaseDir:str='demo/')->str:
    '''
    绘制海雾平面图
    :param ds: xarray DataSet
    :param baseTime: 起报时间, YYYYMMDDHH
    :param timeStep: 预报时效, 小时
    :param imgBaseDir: 图像保持基础路径
    :return 保存的图像文件路径
    '''
    plt.clf()
    fog = dataArray
    lon=fog.coords['lon'][:]#读取经度
    lat=fog.coords['lat'][:]#读取纬度
    # lons,lats=np.meshgrid(lon,lat)#网格化
    figure = plt.figure(figsize=(16, 9))  # 加载画布
    fogMap = plt.axes(projection=ccrs.PlateCarree())  # 设置投影方式
    shpFilePath01  = os.path.join(root_dir, './data/shapefiles/natural_earth/physical/ne_50m_coastline.shp')
    coastmap = shpreader.Reader(shpFilePath01).geometries()  # 读取地图数据
    fogMap.set_extent([105, 125, 15, 28], crs=ccrs.PlateCarree())  # 设置绘图范围
    fogMap.add_geometries(coastmap, ccrs.PlateCarree(),
                          facecolor='none', edgecolor='black')  # 设置边界样式

    shpFilePath02  = os.path.join(root_dir, './data/shapefiles/china_basic_map/bou2_4l.shp')
    chinamap = shpreader.Reader(shpFilePath02).geometries()  # 读取地图数据
    fogMap.set_extent([105, 125, 15, 28], crs=ccrs.PlateCarree())  # 设置绘图范围
    fogMap.add_geometries(chinamap, ccrs.PlateCarree(),
                          facecolor='none', edgecolor='black')  # 设置边界样式

    
    fogMap.set_xticks([105, 110, 115, 120, 125])  # 需要显示的经度，一般可用np.arange
    fogMap.set_yticks([15, 17.5, 20, 22.5, 25, 27.5])  # 需要显示的纬度
    fogMap.xaxis.set_major_formatter(LongitudeFormatter())  # 将横坐标转换为经度格式
    fogMap.yaxis.set_major_formatter(LatitudeFormatter())  # 将纵坐标转换为纬度格式
    fogMap.tick_params(axis='both', labelsize=15, direction='in',
                       length=5, width=0.55, right=True, top=True)  # 修改刻度样式
    fogMap.grid(linewidth=0.4, color='k', alpha=0.45, linestyle='--')  # 开启网格线
    fogContourf = fogMap.contourf(lon, lat, fog, bound_fog,
                                    cmap=clrmap_fog, norm=norms_fog)
    cb = figure.colorbar(fogContourf, extend='max', shrink=0.8, pad=0.01)
    cb.set_label('能见度 单位: km', fontdict={'size': 14})
    cb.ax.tick_params(which='major', direction='in', length=6, labelsize=15)
    cb.ax.set_yticklabels(np.array(bound_fog)/1000.0)

    # 设置文字标题 Set the titles and axes labels
    utcTime = arrow.get(fog['time'].dt.strftime(
        '%Y-%m-%d %H:%MZ').item(), 'YYYY-MM-DD HH:mmZ')
    initTime = utcTime.shift(hours=8)
    fcTime = initTime.shift(hours=int(timeStep))
    fogMap.set_title(f'海上能见度定量预报 起报:{initTime.format("YYYY-MM-DD HH:00")}, 预报:{fcTime.format("DD日HH时")},(北京时)', fontsize=20)

    #########  保存图像  ###########
    
    imgDir = os.path.join(imgBaseDir, f'./{utcTime.format("YYYY/MM/DDHH/")}')
    imgDir = os.path.normpath(imgDir)
    try:
        createDir(imgDir)
    except Exception as e:
        raise e
    imgPath = os.path.join(imgDir, f'fog_{utcTime.format("YYYYMMDDHH")}_{timeStep}.png')
    plt.savefig(imgPath, format='png', bbox_inches='tight', transparent=True)
    print('保存图片: ' + imgPath)
    plt.close(figure)
    return imgPath