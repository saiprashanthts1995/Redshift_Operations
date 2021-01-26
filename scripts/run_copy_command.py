__Author__ = 'Sai Prashanth Thalanayar Swaminathan'
__email__ = 'saiprashanthts@gmail.com'
__purpose__ = 'Run the copy command'
from utils import read_col_specs, udf_log, read_config, read_table_details, redshift_connection
import sys
import sqlparse
import sys


class RunCopy:
    def __init__(self, section, table_name, log):
        """
        Constructor to initiate the RunCOpy Class
        :param section: section name
        :param table_name:  table name
        :param log: log for logging the module
        """
        self.section = section
        self.table_name = table_name
        self.log = log

    def copy_command(self) -> str:
        """
        This UDF is used to generate the Copy command based on the instance parameters passed
        :return:
        """
        details = read_table_details(self.section, self.table_name, self.log)
        print(details['copy_command'])
        if 'copy_command' in details:
            command = details['copy_command'].format(**details)
        else:
            self.log.error('Copy command missing for {}'.format(self.table_name))
            sys.exit(1)
        self.log.info("Generated the copy command")
        self.log.info("Command is as follows")
        self.log.info("{}".format(sqlparse.format(command, reindent=True, keyword_case='upper')))
        return command

    def run_command(self, command: str):
        """
        This UDF is used to run the copy command
        :param command:
        :return:
        """
        try:
            connection = redshift_connection(self.section, self.table_name, self.log)
            cursor = connection.cursor()
            cursor.execute(command)
        except Exception as ex:
            print(ex)
        finally:
            connection.commit()
            connection.close()
        self.log.info('Connection established successfully')
        return True

    def main(self):
        """
        This udf is used to call all above instance methods and complete the copy process
        :return:
        """
        copy_command = self.copy_command()
        if self.run_command(copy_command):
            print(f'{self.table_name} copied Successfully ')
        self.log.info('Copy command executed successfully')


if __name__ == '__main__':
    log = udf_log('default.log')
    log.info('Welcome! This is useful for loading file into redshift')
    section = 'COPY'
    table_name = 'Customers'
    copy_obj = RunCopy(section, table_name, log)
    copy_obj.main()
