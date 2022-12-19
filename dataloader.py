# -*- coding: utf-8 -*-
"""
Dataloader is a python class which preprocess the raw data saves in different csv files in a folder.
Given the folder path, DataLoader identifies all readable csv files and saves their names as a list. This list with names
is used to load particular data using the function "read_data"
In addition, it provides functions to merge and export all raw data into a single pkl file so as to simplify the
dataloader process.

Install missing python files with pip-install

Example:
    $ python3 TBD

.. _Google Python Style Guide:
   https://google.github.io/styleguide/pyguide.html

@Author: Xing Li
@Date: 19.Dez.2022
"""


import os
import pandas as pd
import pickle
import numpy as np
from datetime import date, datetime, timedelta
from dateutil.relativedelta import *
from loguru import logger


class DataLoader:

    def __init__(self, data_folder: str = None):
        self.data_folder = data_folder
        self.curt_path = os.getcwd()
        self.readable_file_names = dict()
        self.merged_data_dict = dict()
        self.logger = logger.opt(colors=True)

        self.search_readable_files()

    def search_readable_files(self):
        directory = os.fsencode(self.data_folder)
        for file in os.listdir(directory):
            filename = os.fsdecode(file)
            if filename.endswith(".csv"):
                key = filename.split(".")[0]
                self.readable_file_names[key] = os.path.join(self.data_folder, filename)
                self.logger.info('Found readable file {}'.format(filename))

    def read_data(self, file_name):
        file_path = self.readable_file_names[file_name]
        data = pd.read_csv(file_path)
        return data

    def merge_all_data(self, export_data=True):
        """ load all data and merge them as a dictionary
        """
        self.merged_data_dict = dict()
        for key in self.readable_file_names:
            self.merged_data_dict[key] = self.read_data(key)
        self.logger.info('Finish Merging')
        if export_data:
            pkl_data_path = os.path.join(self.data_folder, "merged_data.pkl")
            with open(pkl_data_path, 'wb') as handle:
                pickle.dump(self.merged_data_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)
                self.logger.info("output pickle file to {}".format(pkl_data_path))

    def load_merged_data_from_pkl(self, pkl_data_path):
        with open(pkl_data_path, 'rb') as handle:
            self.merged_data_dict = pickle.load(handle)
            self.logger.info('Loading merged data')


def main():
    data_folder_path = os.path.join(os.getcwd(), "industry-related-data")
    dataloader = DataLoader(data_folder=data_folder_path)
    dataloader.merge_all_data()
    print('Testing')


if __name__ == "__main__":
    main()