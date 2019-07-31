import csv
import numpy as np
import logging
import requests
import json
import copy
from metadata import Metadata



class StreamCatWebService():
    def __init__(self):
        self.metadata = Metadata()

        logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(name)s - %(levelname)s -  %(message)s',
                        datefmt='%d-%b-%y %H:%M:%S',
                        filename='stream_cat_web_service.log',
                        filemode='w')

        self.logger = logging.getLogger("stream_cat_web_service")
        self.logger.info("starting read siteinfo.csv")
    
    

    def log_msg(self, msg):
        logger.info(msg)


    #Read all streamcat csv files    
    def get_streamcat_data(self):

        lst_sites = list()
        with open("site_info_comids.csv", newline = '') as sites_file:
                    
            idx = 0

            #url = https://ofmpub.epa.gov/waters10/streamcat.jsonv25?pcomid=6279029&pLandscapeMetricType=Topography
            url = "https://ofmpub.epa.gov/waters10/streamcat.jsonv25?pcomid={}&pLandscapeMetricType=Topography"

            site_reader = csv.DictReader(sites_file)
            for row in site_reader:
                try:

                    site_id = row["SITE_ID"]
                    log_msg(site_id)
                    year = row["YEAR"]
                    visit_no = row["VISIT_NO"]
                    date_collected = row["DATE_COL"]
                    site_name = row["LOC_NAME"]
                    lat = row["LAT_DD83"]
                    lon = row["LON_DD83"]
                    huc8 = row["HUC8"]
                    huc8 = huc8.zfill(8)
                    row["HUC8"] = huc8
                    comid = row["COMID"]

                    idx = idx + 1
                    qry_str = url.format(comid)

                    response = requests.get(qry_str)
                    if (response.status_code != 200):
                        log_msg("Bad get call")

                    data = response.json()

                    new_row = copy.copy(row)                
                    new_row["ELEVCAT"] = str(data["output"]["metrics"][0]["metric_value"])
                    new_row["ELEVWS"] = str(data["output"]["metrics"][1]["metric_value"])
                    new_row["AREACAT"] = str(data["output"]["areas_of_interest"][0]["areasqkm"])
                    new_row["AREAWS"] = str(data["output"]["areas_of_interest"][1]["areasqkm"])

                    lst_sites.append(new_row)

                    #if (idx > 10):
                    #    break

                    print("Row: " + str(idx))


                except Exception as e:
                    self.log_msg(str(e))

            with open("site_info_comids_elev_area.csv", "w", newline='') as csvfile:
                field_names = ["SITE_ID", "YEAR", "VISIT_NO", "DATE_COL", "LOC_NAME", "LAT_DD83", "LON_DD83", "HUC8", "COMID", "ELEVCAT", "ELEVWS", "AREACAT", "AREAWS"]
                writer = csv.DictWriter(csvfile, fieldnames=field_names, delimiter=',')
                writer.writeheader()
                for row in lst_sites:
                    writer.writerow(row)


if __name__ == '__main__':

    ws_reader = StreamCatWebService()
    ws_reader.get_streamcat_data()
    