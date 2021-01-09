__Author__ = 'Sai Prashanth Thalanayar Swaminathan'
__email__ = 'saiprashanthts@gmail.com'
__purpose__ = 'To create user defined functions which can be leveraged in the other modules'

import json
import loguru
from datetime import datetime
import sys


def read_col_specs(col_spec_file_name):
    """
    This is used to read config of col specification of fixed width file
    :param col_spec_file_name:
    :return:
    """
    with open("../config/{}".format(col_spec_file_name)) as file:
        col_spec_content = json.load(file)
    return col_spec_content


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


def read_config(config_file_name):
    """
    This UDF is used to read the config details of redshift
    :param config_file_name:
    :return:
    """
    config_path = open("../config/{}".format(config_file_name))
    config = json.load(config_path)
    config_path.close()
    return config


def udf_log(log_path):
    pass


if __name__ == "__main__":
    print("hello")
    print(read_col_specs('col_specs_fwf_departments.json'))
    print(read_config("config.json"))

