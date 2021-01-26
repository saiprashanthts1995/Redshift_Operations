__Author__ = 'Sai Prashanth Thalanayar Swaminathan'
__email__ = 'saiprashanthts@gmail.com'
__purpose__ = 'Create External Schema and External table'

from utils import read_col_specs, udf_log, read_config, read_table_details, redshift_connection
import sqlparse
import sys


class CreateExternalTable:
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

    def create_command(self, command_format):
        """
        This is used to generate the command based on the table name
        :param command_format: it can be either create schema or create table
        :return: command
        """
        details = read_table_details(self.section, self.table_name, self.log)
        if 'create_external_schema_command' in details or 'create_external_table_command' in details:
            command = details[command_format].format(**details)
        else:
            self.log.error('command {} missing for {}'.format(command_format, self.table_name))
            sys.exit(1)
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
        self.log.info('{} executed'.format(sqlparse.format(command, reindent=True, keyword_case='upper')))
        return True

    def main(self):
        """
        This Function is used to call subsequent instance methods
        :return:
        """
        for element in ['create_external_schema_command', 'create_external_table_command']:
            query = self.create_command(element)
            print(query)
            self.run_command(query)
            self.log.info('{} command ran fine!'.format(element))


if __name__ == '__main__':
    log = udf_log('default.log')
    log.info('Welcome! This is useful for loading file into redshift')
    section = 'CREATE_EXTERNAL'
    table_name = 'Iris'
    copy_obj = CreateExternalTable(section, table_name, log)
    copy_obj.main()
