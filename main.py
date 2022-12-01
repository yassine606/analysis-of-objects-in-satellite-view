import os
from utils import football_field_data


if __name__ == "__main__":
   path = os.getcwd()
   path_to_json = path + '/'+ 'versailles.json'
   ft = football_field_data(path_to_json)
   ft.process()
