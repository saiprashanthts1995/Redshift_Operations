__Author__ = 'Sai Prashanth Thalanayar Swaminathan'
__email__ = 'saiprashanthts@gmail.com'

import argparse
from create_external_table import CreateExternalTable
from load_fwf_file import Load_Fixed_Width
from run_copy_command import RunCopy
from utils import udf_log, udf_exception, timeit
import sys


@udf_exception
@timeit
def run(section_name, name_of_the_table, log):
    """
    This is used to run the corresponding scripts based on the input passed
    :param section_name: name of the section
    :param name_of_the_table: table name
    :param log: log
    :return:
    """
    log.info('Process Started for the section {} and for the table'.format(section_name, name_of_the_table))
    if section_name == 'COPY':
        RunCopy(section_name, name_of_the_table, log).main()
    elif section_name == 'CREATE_EXTERNAL':
        CreateExternalTable(section_name, name_of_the_table, log).main()
    elif section_name == 'FIXED_WIDTH':
        Load_Fixed_Width(section_name, name_of_the_table, log).main()
    else:
        print('Modules Not Found')
    log.info('Process Completed for the section {} and for the table'.format(section_name, name_of_the_table))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Load data to Redshift",
                                     prog="app.py")
    parser.add_argument("--section",
                        "-section",
                        help="Please pass the section",
                        dest="section",
                        choices=['CREATE_EXTERNAL', 'COPY', 'FIXED_WIDTH'],
                        required=True
                        )
    parser.add_argument("--table_name",
                        "-table_name",
                        help="Please pass the table name",
                        dest="table_name",
                        required=True)
    try:
        args = parser.parse_args()
    except Exception as e:
        parser.print_help()
        sys.exit(1)

    # setting the arguments for the script
    section = args.section
    table_name = args.table_name
    log = udf_log('default.log')
    log.info('Welcome! This is useful for loading file into redshift')
    run(section, table_name, log)

