import os
import csv
import sys
import logging
import math
import sqlite3
import numpy as np
from metadata import Metadata


class StreamCatFileReader():

    def __init__(self):
        self.metadata = Metadata()

        logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(name)s - %(levelname)s -  %(message)s',
                        datefmt='%d-%b-%y %H:%M:%S',
                        filename='streamcat_csv_data.log',
                        filemode='w')

        self.log_msg("Starting")


    def log_msg(self, msg):            
        logging.debug(msg)

    def get_input_types(self):
        dtypes = [  np.dtype('<U20'),      #SITE_ID
                    np.dtype(np.int),       #YEAR
                    np.dtype(np.int),       #VISIT_NO
                    np.dtype('<U12'),       #DATE_COL
                    np.dtype('<U30'),       #LOC_NAME
                    np.dtype(np.float),     #LAT
                    np.dtype(np.float),     #LON
                    np.dtype('U8'),         #HUC8
                    np.dtype(np.int),       #COMID
                    np.dtype(np.float),     #ELEVCAT
                    np.dtype(np.float),     #ELEVWS
                    np.dtype(np.float),     #AREACAT
                    np.dtype(np.float)]     #AREAWS

        return dtypes

    def write_data(self, file_type, header, data):
        filename = "Output/{}/{}.csv".format(file_type, file_type)
        
        with open(filename, "w", newline='') as csvfile:
                writer = csv.writer(csvfile, delimiter=',', )
                writer.writerow(header)
                for row in data:
                    writer.writerow(row)
        
        

    def get_data(self):

        file_types = self.metadata.get_file_types()
        file_format = self.metadata.get_file_format()
        variables = self.metadata.get_variables()

        #Build region list. Files have been combined so one file easch region
        regions = list()
        for reg in range(1,19):
            regions.append(str(reg).zfill(2))            

        dtypes = self.get_input_types()

        arr = np.genfromtxt("site_info_comids_elev_area.csv", delimiter=',', names=True, dtype=dtypes)
        #Sort the array by HUC8
        arr_sorted_huc8 = np.sort(arr,order="HUC8")

        header_base = ["comid", "huc8"]
        dtypes_base = [np.dtype('<U20'), np.dtype(np.int)]
        #Start outer loop for file types
        for file_type in file_types:
            header = None
            vars = variables[file_type]            
            header = header_base + vars
            dtypes = list()
            for var in vars:
                dtypes.append(np.float)

            dtypes = dtypes_base + dtypes
            out_data_file_type = list()

            #for reg in regions:                                        
                #header = ["comid", "huc8", "chyd", "cchem", "csed", "cconn","ctemp","chabt","whyd","wchem","wsed","wconn","wtemp","whabt"]                                
                
            huc2_current = "00"
            out_data_region = list()

            #This is the numpy array from csv data file
            data_arr = None

            idx_comid_loop = 0
            huc8 = None
            comid = None

            #Handle redundant comids
            dct_comids = dict()

            #Loop over the sites
            for row in np.nditer(arr_sorted_huc8):
                #new data row we are extracting
                row_comid = list()
                huc8 = str(row["HUC8"]).strip()
                huc8_len = len(huc8)
                comid = str(row["COMID"]).strip()
                print("COMID: " + comid)
                if (comid not in dct_comids.keys()):
                    dct_comids[comid] = comid
                else:
                    continue

                row_comid.append(comid)
                row_comid.append(huc8)
                
                #Get first 2 digits of HUC8
                digit_2 = huc8[:2]

                #Load next file if we have moved to next HUC2
                if (digit_2 != huc2_current):
                    print(digit_2)
                    huc2_current = digit_2
                    file_name = file_format.format(file_type, huc2_current, "csv")
                    file_path = os.path.join(file_type, file_name)

                    #Load the csv data file
                    data_arr = np.genfromtxt(file_path, delimiter=',', names=True, dtype=None)                        
                    
                idx_comid_loop += 1    
                #print(str(idx_comid_loop))
                icomid = int(comid)

                #Find the index for this COMID
                comid_idx = np.where(data_arr["COMID"] == icomid)
                comid_idx = comid_idx[0]
                #Get the data by index
                data_row = data_arr[comid_idx]                    

                try:
                    if (len(data_row) <1):
                        raise Exception('COMID: {} is missing from region: {}'.format(str(icomid), digit_2))

                    for var in vars:
                        val = None
                        val = data_row[var]
                        row_comid.append(val[0])

                    out_data_region.append(row_comid)
                    

                except:
                    msg = "Error with comid: {} in HUC8: {}"
                    self.log_msg(msg.format(comid, huc8))
                    e = sys.exc_info()[0]
                    self.log_msg(e)            
                                
            self.write_data(file_type, header, out_data_region)
            #out_data_file_type.append(out_data_region)
                #End of comid loop
                        
    
    def add_to_db(self, data_table):
        qry = "insert into stream_cat (comid, wsareasqkm, elevws, popden2010ws, mast_2008, mast_2009, prg_bmmi, iwi_v2_1) values ({}, {}, {}, {}, {}, {}, {}, {})"
        conn = sqlite3.connect(os.path.join("output", "pisces_stream_cat.db"))
        with conn:
            cursor = conn.cursor()
            for row in data_table:
                qry_ins = qry.format(*row)
                cursor.execute(qry_ins)

            conn.commit()
            cursor.close()   

    #This function
    def get_all_comids_data(self):
        regions = list()
        for reg in range(1,19):
            regions.append(str(reg).zfill(2))

        file_types = self.metadata.get_file_types()
        file_format = self.metadata.get_file_format()
        variables = self.metadata.get_variables()

        dct_var_idx = {"comid": 0, "wsareasqkm":1, "elevws":2,"popden2010ws":3, "mast_2008":4,"mast_2009":5,"prg_bmmi":6, "iwi_v2_1":7}
    
        for reg in regions:

            print("Region: " + reg)
            #This list will hold list of rows for a region
            data_table = list()

            #flag to indicate new rows are added not appended
            b_add_flag = True

            for file_type in file_types:
                print("Filetype: " + file_type)

                vars = variables[file_type]
                file_name = file_format.format(file_type, reg, "csv")
                file_path = os.path.join(file_type, file_name)

                #Load the csv data file
                data_arr = np.genfromtxt(file_path, delimiter=',', names=True, dtype=None, encoding=None)
                size = len(data_arr)
                print("Num records: " + str(size))
                for idx in range(0,size):

                    data_row = data_arr[idx]
                    comid = data_row["COMID"]
                    new_row = list()
                    if b_add_flag:
                        new_row.append(comid)

                    for var in vars:
                        val = None
                        #Handle NA values in file
                        try:
                            val = float(data_row[var])
                            if math.isnan(val):
                                val = -9999
                        except ValueError:
                            val = -9999
                        #Append to row in data_table if it is there
                        if b_add_flag:
                            new_row.append(val)
                        else:
                            var_idx = dct_var_idx[var.lower()]
                            lst_row = data_table[idx]
                            lst_row.append(val)
                                                
                    if b_add_flag:
                        data_table.append(new_row)
                
                b_add_flag = False

            self.add_to_db(data_table)






if __name__ == '__main__':
    sc_reader = StreamCatFileReader()
    #sc_reader.get_data()
    sc_reader.get_all_comids_data()
    
