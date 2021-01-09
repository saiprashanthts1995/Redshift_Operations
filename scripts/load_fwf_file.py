__Author__ = 'Sai Prashanth Thalanayar Swaminathan'
__email__ = 'saiprashanthts@gmail.com'
__purpose__ = 'Load fixed width file into redshift cluster'

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text


class Load_Fixed_Width:
    def __init__(self, file_name):
        """
        Constructor to accept the name of the file
        :param file_name:
        """
        self.file_name = file_name

    def read_fixed_width(self):
        data = pd.read_fwf(self.file_name,
                           colspecs={})

    def create_db_connection(self):
        pass

    def load_into_table(self):
        pass

    def main(self):
        pass


if __name__ == '__main__':
    fwf = Load_Fixed_Width('../data/departments_fixed_width_file.txt')
    print(fwf.main())

