import csv
import os
import shutil
import urllib.request as request
from contextlib import closing
from ftplib import FTP
from metadata import Metadata




class DownloadStreamCat():

    def __init__(self):
        self.metadata = Metadata()


    def download_zipfiles(self):

        url_base = "newftp.epa.gov"
        url_path = "EPADataCommons/ORD/NHDPlusLandscapeAttributes/StreamCat/HydroRegions"
        file_common = "_Region{}.zip"

        #file_types = ["Elevation", "USCensus2010",  "RefStreamTempPred", "NRSA_PredictedBioCondition", "ICI_IWI_v2.1"]    
        file_types = self.metadata.get_file_types()
        #regions = ["01", "02","03N", "03S","03W", "04","05", "06","07", "08","09", "10L","10U", "11","12", "13","14","15","16","17","18"]
        regions = self.metadata.get_regions()

        for file_type in file_types:
            
            for reg in regions:
                file_base = file_common.format(reg)
                file_name = file_type + file_base
                url = url_base + file_name
                ftp = FTP(url_base)
                ftp.login()
                ftp.cwd("EPADataCommons")
                ftp.cwd("ORD")
                ftp.cwd("NHDPlusLandscapeAttributes")
                ftp.cwd("StreamCat")
                ftp.cwd("HydroRegions")
                local_filename = os.path.join(file_type, file_name)
                if not os.path.exists(file_type):
                    os.makedirs(file_type)
                #ftp.retrbinary("RETR " + file_name, open(file_name, 'wb').write)
                lf = open(local_filename, "wb")
                ftp.retrbinary("RETR " + file_name, lf.write)
                lf.close()
                ftp.quit()



if __name__ == '__main__':
    dl_streamcat = DownloadStreamCat()
    dl_streamcat.download_zipfiles()


    

            
