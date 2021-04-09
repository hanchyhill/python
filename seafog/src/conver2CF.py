import xarray as xr

config = {
    'sstk':{
        'name':'sstk',
        'missing_value': -999.9,
        '_FillValue':  -999.9,
        'valid_min': 0.0,
        'standard_name': 'sea_surface_skin_temperature',
        'units': 'K',
        'long_name': 'sea surface temperature',
        'short_name': 'SST',
    },
    'visi':{
        'name':'visi',
        'missing_value': -999.9,
        '_FillValue':  -999.9,
        'valid_min': 0.0,
        'standard_name': 'visibility_in_air',
        'units': 'm',
        'long_name': 'visibility',
        'short_name': 'visibility',
    },
    't2md':{
        'name':'t2md',
        'missing_value': -999.9,
        '_FillValue':  -999.9,
        'valid_min': 0.0,
        'standard_name': 'dew_point_temperature',
        'units': 'K',
        'long_name': 'dew point',
        'short_name': 'Td',
    },
    't2mm':{
        'name':'t2mm',
        'missing_value': -999.9,
        '_FillValue':  -999.9,
        'valid_min': 0.0,
        'standard_name': 'air_temperature',
        'units': 'K',
        'long_name': 'air temperature in 2 metre',
        'short_name': 'T2m',
    },
    'rhum':{
        'name':'rhum',
        'missing_value': -999.9,
        '_FillValue':  -999.9,
        'valid_min': 0.0,
        'standard_name': 'relative_humidity',
        'units': '1',
        'long_name': 'relative_humidity',
        'short_name': 'RH',
    },
    'temp':{
        'name':'rhum',
        'missing_value': -999.9,
        '_FillValue':  -999.9,
        'valid_min': 0.0,
        'standard_name': 'air_temperature',
        'units': 'K',
        'long_name': 'air temperature',
        'short_name': 'Temp',
    },
}

file0 = '201810-202101_lon110.25_lat20.125.sstk.nc'
file1 = '202102-202103_lon110.25_lat20.125.sstk.nc'

# ds.fillna(value=values)
# da.where(x < 0, np.nan)
# da.fillna(-999.9)