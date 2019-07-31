import csv
import numpy as np
import logging
import requests
import json
import copy

logger = None

def log_msg(msg):
    logger.info(msg)

if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)s - %(levelname)s -  %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S',
                    filename='stream_cat.log',
                    filemode='w')

    logger = logging.getLogger("stream_cat")
    logger.info("starting read siteinfo.csv")

    lst_sites = list()
    with open("siteinfo.csv", newline = '') as sites_file:
                
        idx = 0

        url = "https://ofmpub.epa.gov/waters10/SpatialAssignment.Service?pGeometry=POINT({} {})&pLayer=NHDPLUS_CATCHMENT&pSpatialSnap=TRUE&pReturnGeometry=FALSE"

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
                
                idx = idx + 1
                qry_str = url.format(lon, lat)

                response = requests.get(qry_str)
                if (response.status_code != 200):
                    log_msg("Bad get call")

                data = response.json()
                comid = data["output"]["assignment_value"]
                comid2 = comid
                new_row = copy.copy(row)
                new_row["COMID"] = comid
                lst_sites.append(new_row)

                print("Row: " + str(idx))


            except Exception as e:
                log_msg(str(e))

        with open("site_info_comids.csv", "w", newline='') as csvfile:
            field_names = ["SITE_ID", "YEAR", "VISIT_NO", "DATE_COL", "LOC_NAME", "LAT_DD83", "LON_DD83", "HUC8", "COMID"]
            writer = csv.DictWriter(csvfile, fieldnames=field_names, delimiter=',')
            writer.writeheader()
            for row in lst_sites:
                writer.writerow(row)



            

        


            
    
    #logger.setLevel(logging.INFO)
    # create file handler which logs even debug messages
    #fh = logging.FileHandler('stream_cat.log')
    #fh.setLevel(logging.DEBUG)
    # create console handler with a higher log level
    #ch = logging.StreamHandler()
    #ch.setLevel(logging.ERROR)
    # create formatter and add it to the handlers
    #formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    #ch.setFormatter(formatter)
    #fh.setFormatter(formatter)
    # add the handlers to logger
    #logger.addHandler(ch)
    #logger.addHandler(fh)
    
