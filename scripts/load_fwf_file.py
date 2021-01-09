__name__ = 'Sai Prashanth Thalanayar Swaminathan'
__email__ = 'saiprashanthts@gmail.com'
__purpose__ = 'Load fixed width file into redshift cluster'

import pandas as pd


class Load_Fixed_Width:
    def __init__(self, file_name):
        self.file_name = file_name

    def read_fixed_width(self):
        pass

    def create_db_connection(self):
        pass

    def load_into_table(self):
        pass

    def main(self):
        pass


if __name__ == '__main__':
    fwf = Load_Fixed_Width('../data/departments_fixed_width_file.txt')
    print(fwf.main())

