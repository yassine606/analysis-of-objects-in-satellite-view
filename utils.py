import numpy as np
import json
import pandas as pd
import requests
import time
from tqdm import tqdm


class football_field_data():

    """Initialization of variables, taking dictionary and based on it's name(city mentioned on path of dictionary), name of city is
    being extracted and and based on the key values starting point and ending point is taken"""
    def __init__(self, path_dictionary):
        self.dict_path = path_dictionary
        f = open(self.dict_path)
        self.Dict = json.load(f)
        self.start_point = int(list(self.Dict.keys())[0])
        self.end_point = int(list(self.Dict.keys())[-1])
        city = self.dict_path.split("/")[-1]
        self.city = city.split(".")[0]
        self.df = pd.DataFrame(
            columns=('Football_field_name', 'Lattitide', 'Longitude', 'Address', 'ZipCode', 'Region'))
        self.urls = []

    def create_urls(self):
        """
        creating urls for Google maps, This url contains a query which takes information as name of sport,
        type of establishment (required in Google maps API to sort results) latitude and longitude of region,
        in which search is being made, radius and API key. This information is used to fetch data from GoogleMaps
        """
        print("...Creating urls")
        for i in tqdm(range(self.start_point, (self.end_point+1))):
            string = "https://maps.googleapis.com/maps/api/place/textsearch/json?query=terrain+de+football&location=" + str(
                self.Dict[str(i)]['lat']) + ',' + str(self.Dict[str(i)][
                                                          'lon']) + "&radius=5000&region=fr&type=stadium&key=AIzaSyDafxCQfYyJsNCdZcDMwJXgrH-K1IuSHeY"
            self.urls.append(string)
        print("Urls Created..")
        return

    def get_data_from_Gmaps(self):

        """
        Here data is being extracted from Google Maps using the urls created, depending upon the number
        of pincodes available in the dictionary. Data is extratced in the raw format and required fields from
        that data is saved.
        """

        c = self.start_point
        football_fields = []
        params = {}
        field_name = []
        field_address = []
        field_lat = []
        field_lon = []
        zip_code = []
        region = []

        print("..Getting Data from GMaps")

        for i in self.urls:
            res = requests.get(i, params=params)
            results = json.loads(res.content)
            football_fields.extend(results['results'])
            time.sleep(2)
            while "next_page_token" in results:
                params['pagetoken'] = results['next_page_token'],
                res = requests.get(i, params=params)
                results = json.loads(res.content)
                football_fields.extend(results['results'])
                time.sleep(2)

            for i in tqdm(range(len(football_fields))):
                field = football_fields[i]
                try:
                    field_name.append(field['name'])
                    zip_code.append(str(c))
                    region.append(self.city)
                except:
                    field_name.append('none')
                    zip_code.append('none')
                    region.append('none')
                try:
                    field_address.append(field['formatted_address'])
                except:
                    field_address.append('none')
                try:
                    field_lat.append(field['geometry']['location']['lat'])
                except:
                    field_lat.append('none')
                try:
                    field_lon.append(field['geometry']['location']['lng'])
                except:
                    field_lon.append('none')
            c = c + 1
        print("Data Extracted from Gmap")
        return field_name, field_lat, field_lon, field_address, zip_code, region

    def unique_name_list(self):
        """
        If Data is extracted for multiple pincodes then data extracted from Google Maps for multiple pincodes
        can be similar, So here we find how many unique sports are present in total in the data.
        """
        names = []
        print("....creating unique names list")
        for i in tqdm(range(len(self.df))):
            name = self.df['Football_field_name'].iloc[i]
            if name not in names:
                names.append(name)
            else:
                continue
        print("unique name list created...")
        return names

    def distance(self, index_list):

        """
       If there is data which falls under more than 1 pincode then under this function, distance is calculated
       from it's nearest pincode. And whose distance is minimum, is taken and others are discarded.
        """
        dist = []
        ind = []
        for i in index_list:
            x = self.df['ZipCode'].iloc[i]
            x_lat = self.Dict[str(x)]['lat']
            x_lon = self.Dict[str(x)]['lon']
            y_lat = self.df['Lattitide'].iloc[i]
            y_lon = self.df['Longitude'].iloc[i]
            distance_lat = abs(x_lat - y_lat)
            distance_lon = abs(x_lon - y_lon)
            distance = distance_lat + distance_lon
            dist.append(distance)
            ind.append(i)
        minimum = min(dist)
        min_index = dist.index(minimum)
        out_index = ind[min_index]

        ind.remove(out_index)

        return out_index, ind

    def find_indices_to_discard(self, names):

        """
       If there is data which falls under more than 1 pincode then under this function, distance is calculated
       from it's nearest pincode. And whose distance is minimum, is taken and others are discarded.Here indices of
       data to be discared is found
        """
        total_ind = []
        diff_ind = []
        for i in range(len(names)):
            same = []
            discard_ind = []
            for j in range(len(self.df)):
                if self.df['Football_field_name'].iloc[j] == names[i]:
                    same.append(j)
            if len(same) > 0:
                true_ind, discard_ind = self.distance(same)
            total_ind = total_ind + discard_ind
        final_discard_ind = list(np.unique(total_ind))
        return final_discard_ind

    def process(self):
        self.create_urls()
        field_name, field_lat, field_lon, field_address, zip_code, region = self.get_data_from_Gmaps()
        self.df['Football_field_name'] = field_name
        self.df['Lattitide'] = field_lat
        self.df['Longitude'] = field_lon
        self.df['Address'] = field_address
        self.df['ZipCode'] = zip_code
        self.df['Region'] = region
        names = self.unique_name_list()
        indices_to_discard = self.find_indices_to_discard(names)
        self.df.drop(self.df.index[indices_to_discard], inplace=True)
        print("..Saving data to csv")
        self.df.to_csv(self.city+'.csv')




