import datetime
import logging
import os
import requests
from typing import Optional

import pandas as pd
import xarray as xr
import datetime as dt

from osgeo import gdal
from ..downloader_http import downloaderHTTP
from ...lib.basic import fill_template, format_string

class CHIRPS_Downloader(downloaderHTTP):
    def __init__(self, product: str, start_time: dt.datetime, end_time: dt.datetime, bbox: list, output, tmp_path, time_template) -> None:
        if self.product == "CHIRPSp25-daily":
            self.url_blank = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/tifs/p25/{data_daily_year}/chirps-v2.0.{data_daily_time}.tif.gz"
            self.url_prelim_blank = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/prelim/global_daily/tifs/p25/{%Y}/chirps-v2.0.{data_daily_time}.tif"
            self.freq = "D"
            self.prelim_nodata = -1
        elif self.product == "CHIRPSp05-daily":
            self.url_blank = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/tifs/p05/{data_daily_year}/chirps-v2.0.{data_daily_time}.tif.gz"
            self.url_prelim_blank = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/prelim/global_daily/tifs/p05/{data_daily_year}/chirps-v2.0.{data_daily_time}.tif"
            self.freq = "D"
            self.prelim_nodata = -9999
        elif self.product == "CHIRPSp25-monthly":
            self.url_blank = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_monthly/tifs/chirps-v2.0.{data_monthly_time}.tif.gz"
            self.url_prelim_blank = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/prelim/global_monthly/tifs/chirps-v2.0.{data_monthly_time}.tif"
            self.freq = "MS"
            self.prelim_nodata = -9999
        else:
            logging.error(" --> ERROR! Only CHIRPSp25-daily, CHIRPSp05-daily and CHIRPSp25-monthly has been implemented until now!")
            raise NotImplementedError()

        self.nodata = -9999
        self.start_time = start_time
        self.end_time = end_time
        self.output_blank = output
        self.ancillary_fld = tmp_path
        self.bbox = bbox

        time_template.update({"data_daily_year" : "%Y", "data_daily_time" : "%Y.%m.%d", "data_monthly_time" : "%Y.%m"})
        self.time_template = time_template
    def setup_io(self) -> pd.DataFrame:
        output_df = pd.DataFrame(index=self.time_range, columns=["url", "output", "ancillary"])
        for time_now in self.time_range:
            dict_template_filled = fill_template(self.time_template, time_now)
            output_df.loc[time_now, "url"] = format_string(self.url_blank, dict_template_filled)
            output_df.loc[time_now, "preliminar_url"] = format_string(self.url_prelim_blank, dict_template_filled)
            output_df.loc[time_now, "output"] = format_string(self.output_blank, dict_template_filled)
            output_df.loc[time_now, "ancillary"] = format_string(os.path.join(self.ancillary_fld, "temp_" + self.product + "{data_daily_time}.tif" ), dict_template_filled)
        return output_df
    def format_output(self, input, output, flag_preliminar, bbox = None):
        if flag_preliminar:
            pre_string = ""
        else:
            pre_string = "/vsigzip/"

        if bbox is None:
            gdal.Translate(output, pre_string + input, **{"noData": self.nodata, "creationOptions": ['COMPRESS=DEFLATE']})
        else:
            gdal.Translate(output, pre_string + input, bbox=bbox,
                           **{"noData": self.nodata, "creationOptions": ['COMPRESS=DEFLATE']})
    def get_data(self):
        time_range = self.setup_product_time(self.start_time, self.end_time)
        io_dict = self.setup_io()
        missing_times = []

        # Download final data
        for time_now in time_range:
            try:
                self.download(io_dict.loc[time_now, "url"], io_dict.loc[time_now, "ancillary"], 200)
            except:
                logging.error(" --> ERROR! Downloading data failed for " + time_now.strftime("%Y-%m-%d") + "!")
                missing_times = missing_times + [time_now]
                continue

            if os.path.isfile(io_dict.loc[time_now, "ancillary"]):
                logging.info(" --> SUCCESS! Downloaded data for " + time_now.strftime("%Y-%m-%d") + "!")
                if self.bbox is not None:
                    bbox = (self.bbox[0], self.bbox[1], self.bbox[2], self.bbox[3])
                    self.format_output(io_dict.loc[time_now, "output"], io_dict.loc[time_now, "ancillary"], False, bbox)

        # Fill with prelimnary data
        for time_now in missing_times:
            try:
                self.download(io_dict.loc[time_now, "preliminar_url"], io_dict.loc[time_now, "ancillary"], 200)
            except:
                logging.error(" --> ERROR! Downloading data failed for " + time_now.strftime("%Y-%m-%d") + "!")
                continue
            if os.path.isfile(io_dict.loc[time_now, "ancillary"]):
                logging.info(" --> SUCCESS! Downloaded data for " + time_now.strftime("%Y-%m-%d") + "!")
                if self.bbox is not None:
                    bbox = (self.bbox[0], self.bbox[1], self.bbox[2], self.bbox[3])
                    self.format_output(io_dict.loc[time_now, "output"], io_dict.loc[time_now, "ancillary"], True, bbox)
