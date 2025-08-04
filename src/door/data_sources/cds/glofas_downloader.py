import datetime as dt
from typing import Generator
import xarray as xr

from .cds_downloader import CDSDownloader

from d3tools.spatial import BoundingBox
from d3tools import timestepping as ts
from d3tools.timestepping.timestep import TimeStep

class GLOFASDownloader(CDSDownloader):

    source = "GLOFAS"
    name = "GLOFAS_downloader"
    cds_url = 'https://ewds.climate.copernicus.eu/api'

    available_products = ['cems-glofas-historical']

    available_variables = {'soil_wetness_index': {'varname': 'swir', 'agg_method': 'mean'}}
    
    available_agg_methods = ['mean', 'max', 'min', 'sum']
    
    default_options = {
        'variables'   : 'soil_wetness_index',
        'agg_method'  : None,
        'ts_per_year' : 12, # the number of timesteps per year to split the download over #365=daily, 12=monthly, 36=10-daily
        'ts_per_year_agg' : 365
    }

    spatial_ref =  'GEOGCRS["WGS 84",\
                    ENSEMBLE["World Geodetic System 1984 ensemble",\
                        MEMBER["World Geodetic System 1984 (Transit)"],\
                        MEMBER["World Geodetic System 1984 (G730)"],\
                        MEMBER["World Geodetic System 1984 (G873)"],\
                        MEMBER["World Geodetic System 1984 (G1150)"],\
                        MEMBER["World Geodetic System 1984 (G1674)"],\
                        MEMBER["World Geodetic System 1984 (G1762)"],\
                        MEMBER["World Geodetic System 1984 (G2139)"],\
                        ELLIPSOID["WGS 84",6378137,298.257223563,\
                            LENGTHUNIT["metre",1]],\
                        ENSEMBLEACCURACY[2.0]],\
                    PRIMEM["Greenwich",0,\
                        ANGLEUNIT["degree",0.0174532925199433]],\
                    CS[ellipsoidal,2],\
                        AXIS["geodetic latitude (Lat)",north,\
                            ORDER[1],\
                            ANGLEUNIT["degree",0.0174532925199433]],\
                        AXIS["geodetic longitude (Lon)",east,\
                            ORDER[2],\
                            ANGLEUNIT["degree",0.0174532925199433]],\
                    USAGE[\
                        SCOPE["Horizontal component of 3D system."],\
                        AREA["World."],\
                        BBOX[-90,-180,90,180]],\
                    ID["EPSG",4326]]'

    def __init__(self, product = 'cems-glofas-historical') -> None:
        super().__init__(product)

        if product not in self.available_products:
            msg = f'Product {product} not available for GLOFAS'
            self.log.error(msg)
            raise ValueError(msg)

    def set_variables(self, variables: str|list[str]) -> None:
        """
        Set the variables to download.
        """
        if isinstance(variables, str):
            variables = [variables]
        super().set_variables(variables)

        agg_options = self.agg_method
        if not isinstance(agg_options, list):
            agg_options = [agg_options]

        if len(agg_options) != len(variables):
            msg = 'The number of aggregation methods must be the same as the number of variables'
            self.log.error(msg)
            raise ValueError(msg)
        
        for agg, var in zip(agg_options, variables):
            agg = self.check_agg(agg)
            self.variables[var].update({'agg_method': agg})

    def check_agg(self, agg):
        if not isinstance(agg, list): agg = [agg]
        for a in agg:
            if a not in self.available_agg_methods:
                msg = f'Aggregation method {a} not available'
                self.log.error(msg)
                raise ValueError(msg)
        return agg

    def build_request(self,
                      time:ts.TimeRange,
                      space_bounds:BoundingBox) -> dict:
        """
        Make a request for the CDS API.
        """
        
        request = super().build_request(
            time, space_bounds
        )

        # add ERA5 specific parameters
        request.update({
                "system_version": ["version_4_0"],
                "hydrological_model": ["lisflood"],
                "product_type": ["consolidated","intermediate"],
        })

        # convert the year, month and day into hyear, hmonth and hday
        request['hyear'] = request.pop('year')
        request['hmonth'] = request.pop('month')
        request['hday'] = request.pop('day')

        return request

    def get_last_published_date(self, **kwargs) -> dt.datetime:
        now = dt.datetime.now()
        now = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return now - dt.timedelta(days=2)
         
    def _get_data_ts(self,
                     timestep: TimeStep,
                     space_bounds: BoundingBox,
                     tmp_path: str) -> Generator[tuple[xr.DataArray, dict], None, None]:

        all_data = super()._get_data_ts(timestep, space_bounds, tmp_path)

        # loop over the variables
        for var, varopts in self.variables.items():
            varname = varopts['varname']
            varopts['var'] = var

            # find the data for the variable
            for this_data in all_data:
                if varname in this_data:
                    data = this_data
                    break

            vardata = data[varname]

            # filter data to the selected days (we have to do this because the API returns data for longer periods than we actually need)
            inrange = (vardata.time.dt.date >= timestep.start.date()) & (vardata.time.dt.date <= timestep.end.date())
            vardata = vardata.sel(time = inrange)

            # finally, remove non needed dimensions
            vardata = vardata.squeeze()

            # verify that we have all the data we need (i.e. no timesteps of complete nans)!
            time_to_check = timestep.start
            while time_to_check <= timestep.end:
                istoday = vardata.time.dt.date == time_to_check.date()
                this_data = vardata.sel(time = istoday)
                for time in this_data.time:
                    if this_data.sel(time = time).isnull().all():
                        self.log.error(f'  -> Missing data for {var} at time {time:%Y-%m-%d %H:%M}')
                        raise ValueError(f'Missing data for {var} at time {time:%Y-%m-%d %H:%M}')

                time_to_check += dt.timedelta(days=1)

            # remove all GRIB attributes
            for attr in vardata.attrs.copy():
                if attr.startswith('GRIB'):
                    del vardata.attrs[attr]

            # aggregate in the superclass and yield
            yield from self._aggregate_variable(vardata, timestep, varopts)