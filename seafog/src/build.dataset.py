import xarray as xr
import numpy as np
import os
import arrow
import pandas as pd
import glob


if os.environ['COMPUTERNAME'] == 'DESKTOP-EQAO3M5':
  computer_flag = 'home'
else:
  computer_flag = 'office'

if computer_flag == 'home':
  root_dir = "F:/github/pythonScript/seafog/"
else:
  root_dir = "H:/github/python/seafog/"

def linear_vis(x):
    if(x <= 1000.0):
      y = x/1000.0
    elif(x > 1000.0 and x <= 10000.0):
      y = (x-1000.0)/9000.0 + 1.0
    elif( x > 10000.0  and x <= 30000.0):
      y = (x-10000.0)/20000 + 2.0
    else:
      y = 3.0
    return y

# fcHour_list = list(range(0, 72+1, 1)) + list(range(78, 168+1, 6))
fcHour_list = list(range(0, 72+1, 1)) + list(range(78, 120+1, 6))
dir_path =  os.path.normpath(os.path.join(root_dir, './data/netcdf/CFdata/fullhour/hdf'))

def scan_hdf():
    '''扫描文件并组合所有hdf'''
    dirPath = dir_path.replace('\\','/')
    reg =f"{dirPath}/*.hdf"
    path_list = glob.glob(reg)
    df_list = []
    for i_path in path_list:
      print(f'读取{i_path}')
      try:
        store_hdf = pd.HDFStore(i_path, mode='r')
      except:
        print(f'读取HDF文件异常{i_path}')
        continue
      for iHour in fcHour_list:
        try:
          i_df = store_hdf.get(f'df_{iHour:0>3d}')
          df_list.append(i_df)
        except:
          print(f'读取指定时次异常{iHour} in {i_path}')
          continue
    build_dataset(df_list)

def build_dataset(df_list):
    '''
    用于创建数据集。它的具体实现包括以下步骤：
    将传入的多个 DataFrame 对象合并成一个大的 DataFrame，并按照 'init_time' 和 'fc_hour' 进行排序。
    新增一列 'station_vis_linear'，其值为 'station_vis' 经过线性转换后的结果。
    根据 'station_vis_cat' 的值将大的 DataFrame 划分成三个子集 df_fog，df_mist 和 df_clear。
    按照比例划分 df_fog 雾集合，并根据日期切片 df_mist 轻雾和 df_clear 无雾情形。将 fog、mist 和 clear 合并，并随机打散以形成训练集、验证集和测试集。
    增加雾的数量，使得 fog 训练集中的样本数等于 mist 和 clear 的训练集中的样本数之和。同时，保持 mist 和 clear 的数量相同。
    将训练集、验证集和测试集合并到一个大的 DataFrame 中，并存储到 HDF 文件中。
    '''
    df_all = pd.concat(df_list, ignore_index=True)
    df_all.sort_values(by=['init_time','fc_hour'], inplace=True, ignore_index=True)
    df_all['station_vis_linear'] = -1.0
    df_all['station_vis_linear'] = df_all['station_vis'].apply(linear_vis)
    df_fog = df_all.loc[df_all['station_vis_cat'] == 0]
    df_mist =  df_all.loc[df_all['station_vis_cat'] == 1]
    df_clear =  df_all.loc[df_all['station_vis_cat'] == 2]
    print(f'df_fog:{len(df_fog)}  df_mist:{len(df_mist)}  df_clear:{len(df_clear)}')

    # 按照比例划分fog雾集合, 再按照fog年份切片mist轻雾和clear无雾情形
    # fog, mist, clear 比例固定为1:1:1
    # 合并 fog, mist, clear
    # 随机打散
    # train:valid : test = 6:2:2
    partial_train = 6.0/10.0
    partial_valid = 2.0/10.0
    partial_test = 2.0/10.0
    df_fog_train = df_fog[:int(len(df_fog)*partial_train)] # 训练集
    df_fog_valid = df_fog[int(len(df_fog)*partial_train):int(len(df_fog)*partial_train)+int(len(df_fog)*partial_valid)] # 验证集
    df_fog_test  = df_fog[int(len(df_fog)*partial_train)+int(len(df_fog)*partial_valid):] # 测试集
    # 检查fog的日期
    fog_valid_sTime = df_fog_valid['actual_time'].iat[0]
    fog_test_sTime = df_fog_test['actual_time'].iat[0]
    
    df_mist_train = df_mist.loc[df_mist['actual_time'].values<fog_valid_sTime]
    df_mist_valid = df_mist.loc[(df_mist['actual_time'].values>=fog_valid_sTime) & (df_mist['actual_time'].values<fog_test_sTime)]
    df_mist_test = df_mist.loc[df_mist['actual_time'].values>=fog_test_sTime]
    
    df_clear_train = df_clear.loc[df_clear['actual_time'].values<fog_valid_sTime]
    # df_clear_valid = df_clear.loc[(df_clear['actual_time'].values>=fog_valid_sTime) & (df_clear['actual_time'].values<fog_test_sTime)]
    df_clear_valid = df_clear.loc[df_clear['actual_time'].values>=fog_valid_sTime]
    df_clear_test = df_clear.loc[df_clear['actual_time'].values>=fog_test_sTime]

    print(f'df_fog_train :{len(df_fog_train)}     df_fog_valid:{len(df_fog_valid)}     df_fog_test:{len(df_fog_test)}')
    print(f'df_mist_train:{len(df_mist_train)}    df_mist_valid:{len(df_mist_valid)}   df_mist_test:{len(df_mist_test)}')
    print(f'df_clear_train:{len(df_clear_train)}  df_clear_valid:{len(df_clear_valid)}  df_mist_clear:{len(df_clear_test)}')
    # 增加雾的数量
    # df_fog_train_extend = pd.concat([df_fog_train]*1, ignore_index=True)
    # df_fog_valid_extend = pd.concat([df_fog_valid]*1, ignore_index=True)
    df_fog_train_extend = df_fog_train
    df_fog_valid_extend = df_fog_valid
    # fog顺序排列, mist和clear数量保持一致
    df_mist_train = df_mist_train.sample(n=len(df_fog_train_extend), replace=True)
    df_clear_train = df_clear_train.sample(n=len(df_fog_train_extend), replace=True)

    df_mist_valid = df_mist_valid.sample(n=len(df_fog_valid), replace=True)
    df_clear_valid = df_clear_valid.sample(n=len(df_fog_valid), replace=True)

    df_mist_test = df_mist_valid.sample(n=len(df_fog_test), replace=True)
    df_clear_test = df_clear_valid.sample(n=len(df_fog_test), replace=True)
    
    # 合并fog mist clear
    df_train = pd.concat([df_fog_train_extend, df_mist_train, df_clear_train], ignore_index=True)
    df_valid = pd.concat([df_fog_valid_extend, df_mist_valid, df_clear_valid], ignore_index=True)
    df_test = pd.concat([df_fog_test, df_mist_test, df_clear_test], ignore_index=True)

    # resample train and valid dataset
    df_train = df_train.sample(frac=1)
    # df_valid = df_valid.sample(frac=1)

    # remove interp. time
    real_fcHour_list = list(range(0, 72+1, 3)) + list(range(78, 168+1, 6))
    df_test_real = df_test[df_test['fc_hour'].isin(real_fcHour_list) ]

    # concat valid and test datasets
    df_valid_test = pd.concat([df_valid, df_test], ignore_index=True)

    fog_dataset_hdf = os.path.normpath(os.path.join(dir_path, './fog_dataset_hdf66_fc120h.h5'))
    store_dataset = pd.HDFStore(fog_dataset_hdf, mode='w')
    df_train.to_hdf(store_dataset, key=f'train', mode='a')
    df_valid.to_hdf(store_dataset, key=f'valid', mode='a')
    df_test_real.to_hdf( store_dataset, key=f'test' , mode='a')
    df_valid_test.to_hdf( store_dataset, key=f'valid_test' , mode='a')
    store_dataset.close()

scan_hdf()