import copy


class Metadata():
    def __init__(self):
        self.file_format = "{}_Region{}.{}"
        self.file_types = ["Elevation", "USCensus2010",  "RefStreamTempPred", "NRSA_PredictedBioCondition", "ICI_IWI_v2.1"]
    
        self.regions = ["01", "02","03N", "03S","03W", "04","05", "06","07", "08","09", "10L","10U", "11","12", "13","14","15","16","17","18"]      

        self.variables = {
                            "Elevation":["WsAreaSqKm","ElevWs"],
                            "USCensus2010":["PopDen2010Ws"],
                            "RefStreamTempPred":["MAST_2008","MAST_2009"],
                            "NRSA_PredictedBioCondition":["prG_BMMI"],
                            "ICI_IWI_v2.1" : ["IWI_v2_1"]
        }        

    def get_file_types(self):
        return list(self.variables.keys())

    def get_variables(self):
        return copy.copy(self.variables)

    def get_regions(self):
        return self.regions.copy()

    def get_file_format(self):
        return self.file_format
