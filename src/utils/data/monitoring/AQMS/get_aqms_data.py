"""
.. module:: aqms_api
   :platform: Unix
   :synopsis: Everything needed to use the api to query aqms.

.. moduleauthor:: Xavier Barthelemy <xavier.barthelemy@environment.nsw.gov.au>


"""

import os
import sys
import requests
import logging
import urllib
import datetime as dt
import json


###########################################################################################
class aqms_api_class(object):
    """
    This class defines and configures the api to query the aqms database
    """

    def __init__(
        self,
    ):

        self.logger = logging.getLogger(__file__)
        # self.url_api = "https://dpe-im-api-airquality-uat.azurewebsites.net"
        self.url_api = "https://data.airquality.nsw.gov.au"
        self.headers = {
            "content-type": "application/json",
            "accept": "application/json",
        }

        self.get_site_url = "api/Data/get_SiteDetails"
        self.get_parameters = "api/Data/get_ParameterDetails"
        self.get_observations = "api/Data/get_Observations"
        return

    ###########################################################################################
    def get_site_details(
        self,
    ):
        """
        Build a query to return all the sites details
        """
        query = urllib.parse.urljoin(self.url_api, self.get_site_url)
        # print(query)
        # response = requests.post(url = query, data = '')
        response = requests.get(query, headers=self.headers)
        return response

    ###########################################################################################
    def get_parameters_details(
        self,
    ):
        """
        Build a query to return all the sites details
        """
        query = urllib.parse.urljoin(self.url_api, self.get_parameters)
        # print(query)
        # response = requests.post(url = query, data = '')
        response = requests.get(url=query, headers=self.headers)
        return response

    ###########################################################################################
    def get_historical_obs(self, ObsRequest):
        """
        Build a query to return all the sites details
        """
        query = urllib.parse.urljoin(self.url_api, self.get_observations)

        response = requests.post(
            url=query, data=json.dumps(ObsRequest), headers=self.headers
        )
        return response

    ###########################################################################################
    def get_now_obs(
        self,
    ):
        """
        Build a query to return all the sites details
        """
        query = urllib.parse.urljoin(self.url_api, self.get_observations)

        response = requests.post(url=query, data="", headers=self.headers)
        return response

    ###########################################################################################
    def ObsRequest_init(
        self,
    ):
        """
        Build a empty dictionary to ready to post to get the obs
        """
        ObsRequest = {}
        ObsRequest["Parameters"] = []
        ObsRequest["Sites"] = []
        ObsRequest["StartDate"] = ""
        ObsRequest["EndDate"] = ""
        ObsRequest["Categories"] = []
        ObsRequest["SubCategories"] = []
        ObsRequest["Frequency"] = []

        return ObsRequest


###########################################################################################

# if __name__ == "__main__":
#     import pandas as pd

#     AQMS = aqms_api_class()

#     ObsRequest = AQMS.ObsRequest_init()
#     StartDate = dt.datetime(2024, 5, 23).strftime("%Y-%m-%d")
#     EndDate = dt.datetime(2024, 5, 25).strftime("%Y-%m-%d")
#     station_id = 1141
#     # AllSites = AQMS.get_site_details()
#     # print(pd.json_normalize(AllSites.json()))

#     # for i, site in enumerate(AllSites.json()):
#     #     # print(i, site)
#     #     ObsRequest["Sites"].append(site["Site_Id"])
#     # # print(ObsRequest["Sites"])
#     # Allparameters = AQMS.get_parameters_details()
#     # print(pd.json_normalize(Allparameters.json()))
#     # for i, param in enumerate(Allparameters.json()):
#     #     # print(i, param)
#     #     ObsRequest["Parameters"].append(param["ParameterCode"])
#     #     ObsRequest["Categories"].append(param["Category"])
#     #     ObsRequest["SubCategories"].append(param["SubCategory"])
#     #     ObsRequest["Frequency"].append(param["Frequency"])

#     # # make all list unique
#     # ObsRequest["Parameters"] = list(set(ObsRequest["Parameters"]))
#     # ObsRequest["Categories"] = list(set(ObsRequest["Categories"]))
#     # ObsRequest["SubCategories"] = list(set(ObsRequest["SubCategories"]))
#     # ObsRequest["Frequency"] = list(set(ObsRequest["Frequency"]))

#     ObsRequest["Sites"] = [1141]
#     ObsRequest["Parameters"] = ["PM2.5", "PM10", "TEMP", "HUMID"]
#     ObsRequest["Categories"] = ["Averages"]
#     ObsRequest["SubCategories"] = ["Hourly"]
#     ObsRequest["Frequency"] = ["Hourly average"]
#     ObsRequest["StartDate"] = StartDate
#     ObsRequest["EndDate"] = EndDate

#     # ObsRequest_now = AQMS.ObsRequest_init()
#     print(ObsRequest)
#     AllObs = AQMS.get_historical_obs(ObsRequest)
#     # AllObs = AQMS.get_Obs(ObsRequest_now)
#     # ObsRequest['Sites'] = [190]
#     # ObsRequest['Parameters'] =  ['WDR']
#     # ObsRequest['Categories'] =  ['Averages']
#     # ObsRequest['SubCategories'] = ['Hourly']
#     # ObsRequest['Frequency'] =  ['Hourly average']

#     # AllObs = AQMS.get_Obs(ObsRequest)
#     # print(json.dumps(ObsRequest))
#     df = pd.json_normalize(
#         AllObs.json(),
#     )
#     # print(AllObs.json())
#     print(df.columns)
#     # print(df)
#     # writing to csv file
#     # today = datetime(2024, 4, 30).strftime("%Y%m%d")
#     # folderpathdir = rf"D:\UTS\OneDrive - UTS\HDR\Papers\Dependability\Coding\sensor-reliability\data\{today}\raw"
#     folderpathdir = rf"data\aqms\raw"
#     if not os.path.exists(folderpathdir):
#         os.makedirs(folderpathdir)

#     filename = folderpathdir + rf"\aqms_{station_id}_{StartDate}-{EndDate}.csv"
#     # Check if the file already exists
#     if os.path.exists(filename):
#         # Append the DataFrame to the existing CSV file without writing the header
#         df.to_csv(filename, mode="a", index=True, header=False)
#     else:
#         # Write the DataFrame to a new CSV file with the header
#         df.to_csv(filename, index=True, header=True)
#     # print(pd.json_normalize(AQMS.get_now_obs().json()))
#     # for i, obs in enumerate(AllObs.json()):
#     #     print(i, obs.text)
