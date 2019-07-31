import zipfile
import os
import pandas as pd
from metadata import Metadata

class UnzipAndMergeStreamCatFiles():
    def __init__(self):
        self.metadata = Metadata()

    def unzip_files(self):
        file_base = self.metadata.get_file_format()
    
        file_types = self.metadata.get_file_types()
        #regions = ["01", "02","03N", "03S","03W", "04","05", "06","07", "08","09", "10L","10U", "11","12", "13","14","15","16","17","18"]        
        regions = self.metadata.get_regions()

        for file_type in file_types:

            for reg in regions:
                file_name = file_base.format(file_type, reg, "zip")
                zip_file = os.path.join(file_type, file_name)        
                zip_ref = zipfile.ZipFile(zip_file, 'r')
                zip_ref.extractall(file_type + "/")
                zip_ref.close()

    def merge_divided_files(self):
        #Only region 3 and region 10 have multiple files that need merged
        file_base = self.metadata.get_file_format()
        file_types = self.metadata.get_file_types()        

        for file_type in file_types:

            file_name03N = os.path.join(file_type, file_base.format(file_type, "03N", "csv"))
            file_name03S = os.path.join(file_type, file_base.format(file_type, "03S", "csv"))
            file_name03W = os.path.join(file_type, file_base.format(file_type, "03W", "csv"))

            file_names = list()
            file_names.append(file_name03N)
            file_names.append(file_name03S)
            file_names.append(file_name03W)

            merged_csv = pd.concat([pd.read_csv(f) for f in file_names ])
            merged_csv.to_csv(os.path.join(file_type, file_type + "_Region03.csv"),index=False)

            file_name10L = os.path.join(file_type, file_base.format(file_type, "10L", "csv"))
            file_name10U = os.path.join(file_type, file_base.format(file_type, "10U", "csv"))

            file_names = list()
            file_names.append(file_name10L)
            file_names.append(file_name10U)

            merged_csv = None
            merged_csv = pd.concat([pd.read_csv(f) for f in file_names ])
            merged_csv.to_csv(os.path.join(file_type, file_type + "_Region10.csv"),index=False)



if __name__ == '__main__':

    uzip_merge = UnzipAndMergeStreamCatFiles()
    #uzip_merge.unzip_files()
    uzip_merge.merge_divided_files()
