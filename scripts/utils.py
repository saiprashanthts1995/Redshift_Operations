__Author__ = 'Sai Prashanth Thalanayar Swaminathan'
__email__ = 'saiprashanthts@gmail.com'
__purpose__ = 'To create user defined functions which can be leveraged in the other modules'

import json
from loguru import logger
from datetime import datetime
import sys
import pandas as pd
import yaml
import psycopg2


def read_table_details(section, table_name, log):
    """
    This UDF is used to read the table details of the section which we wish to load
    :param section:
    :param table_name:
    :param log:
    :return:
    """
    with open('../config/tbl_details.yaml') as f:
        tbl_details = yaml.load(f)
    log.info('tbl_details.yaml read completed. Details loaded for section {}'.format(section))
    return tbl_details['Table_Details'][section][table_name]


def read_col_specs(section, table_name, log):
    """
    This is used to read config of col specification of fixed width file
    :param section:
    :param table_name:
    :param log:
    :return:
    """
    tbl_details = read_table_details(section, table_name, log)
    col_spec_file_name = tbl_details['specification_name']
    with open("{}".format(col_spec_file_name)) as file:
        col_spec_content = json.load(file)
    col_df = pd.DataFrame(data=col_spec_content.items(), columns=['name', 'widths'])
    col_name = col_df['name'].values.tolist()
    widths = col_df['widths'].values.tolist()
    log.info('Column Specification for Fixed width is shown below {}'.format(col_spec_content))
    return [col_name, widths]


def timeit(main_method):
    """
    This decorator is used to find how much time a function takes to execute
    :param main_method:
    :return:
    """
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        result = main_method(*args, **kwargs)
        end_time = datetime.now()
        print("Total time taken is {}".format(end_time - start_time))
        return result
    return wrapper


def udf_exception(main_method):
    """
    This decorator is used to handle user defined exception
    :param main_method:
    :return:
    """
    def wrapper(*args, **kwargs):
        try:
            result = main_method(*args, **kwargs)
            return result
        except Exception as e:
            print(e)
            sys.exit(1)
    return wrapper


def read_config(section, table_name, log):
    """
    This UDF is used to read the config details of redshift
    :param section:
    :param table_name:
    :param log:
    :return:
    """
    tbl_details = read_table_details(section, table_name, log)
    config_name = tbl_details['config_name']
    config_path = open("{}".format(config_name))
    config = json.load(config_path)
    log.info("config.json read successfully")
    config_path.close()
    return config


def udf_log(log_name):
    """
    This is an user defined function which is used to log all activities
    :param log_name:
    :return:
    """
    logger.add(f"../logs/{log_name}",
               retention="10 days",
               rotation="10 days",
               level="INFO")
    return logger


def redshift_connection(section, table_name, log):
    """
    This is used to create connection to redshift using pscyopg2 package
    :param section:
    :param table_name:
    :param log:
    :return:
    """
    config = read_config(section, table_name, log)
    connection = psycopg2.connect(host=config['host'],
                                  port=config['port'],
                                  database=config['db_name'],
                                  username=config['user'],
                                  password=config['password'])
    return connection


if __name__ == "__main__":
    log = udf_log('default.log')
    log.info('Welcome! This is useful for loading file into redshift')
    section = 'FIXED_WIDTH'
    table_name = 'Departments'
    print(read_table_details(section, table_name, log))
    print(read_col_specs(section, table_name, log))
    print(read_config(section, table_name, log))
