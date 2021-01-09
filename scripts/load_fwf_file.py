__Author__ = 'Sai Prashanth Thalanayar Swaminathan'
__email__ = 'saiprashanthts@gmail.com'
__purpose__ = 'Load fixed width file into redshift cluster'

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
from utils import read_col_specs, udf_log, read_config


class Load_Fixed_Width:
    def __init__(self, file_name, log):
        """
        Constructor to accept the name of the file
        :param file_name:
        """
        self.file_name = file_name
        self.log = log

    def read_fixed_width(self, spec_path):
        """
        This function is used to read the fixed width file
        :param spec_path:
        :return:
        """
        # to get specification details of the fixed width file
        spec_details = read_col_specs(spec_path, self.log)

        # read the fixed width file by providing the arguments in the widths as list of position
        data = pd.read_fwf(self.file_name,
                           widths=spec_details[1])

        # changing the column names of dataframe
        data.columns = spec_details[0]
        self.log.info('Data frame after reading first 5 rows is shown below {}'.format(data.head()))
        return data

    def create_db_connection(self, config_path):
        """
        This is used to create db connection
        :param config_path:
        :return: Connection
        """
        config = read_config(config_path, self.log)
        config = config.get('Redshift_Dev')
        engine = create_engine("postgres://{user}:{password}@{host}:{port}/{dbname}".format(**config))
        connection = engine.connect()
        self.log.info('Connection to Redshift has got successfully connected')
        return connection

    def load_into_table(self, data, connection, table_name, schema):
        """
        Loading data into Postgres table
        :param data:
        :param connection:
        :param table_name:
        :param schema:
        :return:
        """
        data.to_sql(name=table_name,
                    schema=schema,
                    con=connection,
                    if_exists='replace')
        self.log.info('Data loaded to table {} completed successfully'.format(table_name))
        return "Success"

    def main(self, spec_path, config_path, table_name, schema):
        """
        This is the main function to call subsequent class functions
        :param spec_path:
        :param config_path:
        :param table_name:
        :param schema:
        :return:
        """
        self.log.info('Process of loading file to Redshift started successfully')
        data = self.read_fixed_width(spec_path)
        connection = self.create_db_connection(config_path)
        self.load_into_table(data, connection, table_name, schema)


if __name__ == '__main__':
    log = udf_log('default.log')
    log.info('Welcome! This is useful for loading file into redshift')
    fwf = Load_Fixed_Width('../data/departments_fixed_width_file.txt', log)
    print(fwf.main('col_specs_fwf_departments.json', 'config.json', 'departments', 'practice'))
    # import logging
    # logging.basicConfig(filename='dummy',
    #                     level=logging.INFO,
    #                     format())
    # logging.info('Hi')
    # logging.warning('Hello')

