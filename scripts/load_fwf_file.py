__Author__ = 'Sai Prashanth Thalanayar Swaminathan'
__email__ = 'saiprashanthts@gmail.com'
__purpose__ = 'Load fixed width file into redshift cluster'

import pandas as pd
from sqlalchemy import create_engine
from utils import read_col_specs, udf_log, read_config, read_table_details


class Load_Fixed_Width:
    def __init__(self, section, table_name, log):
        """
        Constructor is used to set the section, table name and log
        :param section: section name
        :param table_name: table name
        :param log: log object
        """
        self.section = section
        self.table_name = table_name
        self.log = log

    def read_fixed_width(self):
        """
        This function is used to read the fixed width file
        :return:
        """
        # to get specification details of the fixed width file
        spec_details = read_col_specs(self.section, self.table_name, self.log)

        # read the fixed width file by providing the arguments in the widths as list of position
        filename = read_table_details(self.section, self.table_name, self.log)['file_name']
        print(filename)
        data = pd.read_fwf(filename,
                           widths=spec_details[1]
                           )

        # changing the column names of dataframe
        data.columns = spec_details[0]
        self.log.info('Data frame after reading first 5 rows is shown below {}'.format(data.head()))
        return data

    def create_db_connection(self):
        """
        This is used to create db connection
        :return: Connection
        """
        config = read_config(self.section, self.table_name, self.log)
        config = config.get('Redshift_Dev')
        engine = create_engine("postgres://{user}:{password}@{host}:{port}/{dbname}".format(**config))
        connection = engine.connect()
        self.log.info('Connection to Redshift has got successfully connected')
        return connection

    def load_into_table(self, data, connection):
        """
        Loading data into Postgres table
        :param data:
        :param connection:
        :return:
        """
        tbl_details = read_table_details(self.section, self.table_name, self.log)
        table_name = tbl_details['table_name']
        schema = tbl_details['schema']
        data.to_sql(name=table_name,
                    schema=schema,
                    con=connection,
                    if_exists='replace',
                    index=False)
        self.log.info('Data loaded to table {} completed successfully'.format(table_name))
        return "Success"

    def main(self):
        """
        This is the main function to call subsequent class functions
        :return:
        """
        self.log.info('Process of loading file to Redshift started successfully')
        data = self.read_fixed_width()
        connection = self.create_db_connection()
        self.load_into_table(data, connection)


if __name__ == '__main__':
    log = udf_log('default.log')
    log.info('Welcome! This is useful for loading file into redshift')
    section = 'FIXED_WIDTH'
    table_name = 'Departments'
    fwf = Load_Fixed_Width(section, table_name, log)
    print(fwf.main())
    # import logging
    # logging.basicConfig(filename='dummy',
    #                     level=logging.INFO,
    #                     format())
    # logging.info('Hi')
    # logging.warning('Hello')

