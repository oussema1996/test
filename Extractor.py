import json
import logging
import numpy as np
import os
import pandas as pd

DATA_DIR = os.path.relpath("input")
DESCRIPTION_DIR = os.path.relpath("input\input_description")
CSV_Extention = [".csv", ".CSV"]
Excel_Extention = [".xls", ".XLS", ".xlsx", ".XLSX", ".xlsm", ".XLSM"]
Json_Extention = [".json", ".JSON"]


class Extractor():
    """
 Extractor  automate the data extracting tasks from the CSV, Excel, And Json files
 ---------
 Constructor :
 data_dir : the directory of the inputs files, default : DATA_DIR global variable
 description_dir : the directory that contains the description for the inputs files, default : DESCRIPTION_DIR global variable
 files_to_extract : If the user wants to limit the input files, files_to_extract is a list were you can specifie these files
 """
    
    def __init__(self, data_dir=DATA_DIR, description_dir=DESCRIPTION_DIR, files_to_extract=None):
        
        self.data_dir = data_dir
        self.description_dir = description_dir
        self.files_to_extract = files_to_extract

    def __call__(self, *args, **kwargs):
        return self.extract_data()

    """
 method called by Extractor Object
 This function extract only USEFUL data from The files
 """

    def extract_data(self):
        datasets_files = {}
        if self.files_to_extract:
            file_names = self.files_to_extract_validator(self.files_to_extract)
        else:
            dir_path, dir_names, file_names = os.walk(self.data_dir).__next__()
        for file in file_names:
            file_path = os.path.join(self.data_dir, file)
            filename = os.path.basename(file_path)
            filename, file_extention = os.path.splitext(filename)
            description = self.get_description(filename)
            if not os.path.exists(file_path):
                raise Exception("Invalid file")
            else:
                if file_extention in CSV_Extention:
                    data = self.read_csv(file_path)
                if file_extention in Excel_Extention:
                    data = self.read_excel(file_path)
                if file_extention in Json_Extention:
                    data = self.read_json(file_path)
                if filename in datasets_files:
                    datasets_files[filename] = datasets_files[filename].append(data)
                    datasets_files[filename] = self.set_columns(datasets_files[filename], description)
                else:
                    datasets_files[filename] = data
                    datasets_files[filename] = self.set_columns(datasets_files[filename], description)
        return datasets_files

    """
 method called by Extractor Object
 This function gets the description of the Dataframe specified in the ./input/input_description
 """

    def get_description(self, filename):
        file_path = os.path.join(self.description_dir, filename)
        with open(file_path + '.json', encoding="utf-8", mode="r") as json_file:
            data = json_file.read().replace("\n", "")
            description = json.loads(data)
        return description

    """
 method called by Extractor Object
 This function check the type "files_to_extract" since it must be a list
 """

    def files_to_extract_validator(self, files_to_extract):
        if isinstance(files_to_extract, list):
            raise Exception(f"files_to_extract should be a list, got {type(files_to_extract)} instead")
        return files_to_extract

    """
 called when we use to read CSV  files using pandas library
 """

    def read_csv(self, file_path, encoding='utf-8'):
        dataframe = pd.read_csv(file_path, encoding=encoding)
        return dataframe

    """
 called when we use to read Excel  files using pandas library
 """

    def read_excel(self, file_path, encoding='utf-8'):
        dataframe = pd.read_excel(file_path, encoding=encoding)
        return dataframe

    """
 called when we use to read Json files using pandas library
 """

    def read_json(self, file_path):
        with open(file_path, "r") as json_file:
            try:
                df = pd.read_json(json_file)
                return df
            except Exception:
                logging.exception(f"Invalid Json format. Check and try again")

    """
 Methode called to perform some modification in the dataframe depending on the Description 
 """
    def set_columns(self, df, columns):
        # In my approach any missing data can affect the result
        # Missing id,date,title,specific_title,journal,data means that something wrong with the source of our data
        # since all the column will be used and affect all the process
        # if not our data pipeline can adapt very well to this change since we can add a variable in a list in the
        # description json for each column to tell if this column can accept being null or not just like for duplicated
        # rows that you will see later in this function
        df = df[columns.keys()].replace(r'^\s*$', np.nan, regex=True).dropna()
        unique_columns = []
        for column, type in columns.items():
            if type[0] == "id":
                # id must be unique for each data frame
                df.drop_duplicates(subset=[column], keep=False, inplace=True)
            if type[0] == "date":
                # Changing date  format
                df[column] = pd.to_datetime(df[column]).dt.strftime('%d-%m-%Y')
            if type[0] == "string":
                # decode Strings column
                df[column] = df[column].apply(lambda x: x.encode("ascii", "ignore").decode())
                df[column] = df[column].astype(str)
            if type[0] == "numeric":
                # and we validate the type for each column just by specifing the type in the json of the description
                df[column] = df[column].astype(int)
            if type[1]:
                # the second element in  list that describe the column in the json description tell if
                # the column belongs to combination that must be unique or not
                # 1 if in  ,0 if not
                unique_columns.append(column)
        df.drop_duplicates(subset=unique_columns, keep=False, inplace=True)
        return df
