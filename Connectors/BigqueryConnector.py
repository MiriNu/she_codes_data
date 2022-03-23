from google.cloud import bigquery
from google.oauth2 import service_account

import pandas as pd
from datetime import date, timedelta, datetime
from google.cloud import exceptions


def check_types(arg_list, expected_types):
    if len(arg_list) != len(expected_types):
        raise ValueError("The expected type array length does not match the list to check length")
    for i in range(len(arg_list)):
        if type(arg_list[i]) != expected_types[i]:
            raise TypeError("Illegal type, expected types are:" + str(expected_types))


def increase_date_by_day(to_date):
    to_date_as_date = datetime.strptime(to_date, '%Y-%m-%d').date()
    to_date = to_date_as_date + timedelta(days=1)
    return to_date


class BigqueryConnector:

    def __init__(self, json_path):
        credentials = service_account.Credentials.from_service_account_file(
            json_path, scopes=["https://www.googleapis.com/auth/cloud-platform"])
        self.db_name = credentials.project_id
        self.client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    def get_dim_full_table(self, scheme_name, table_name):
        """
        Retrieves the full wanted table from bigQuery, as pandas df.
        :param scheme_name: string represents the scheme name ("mrr", "dwh", etc.)
        :param table_name: string represents the table name.
        :return: df with all the table's rows.

        :raise TypeError when any arg is not a string
        :raise google.api_core.exception when error occurred.

        """
        check_types([scheme_name, table_name], [str, str])
        full_table_id = self.build_table_id(scheme_name, table_name)
        sql = """
                SELECT *
                FROM `%s`
            """ % full_table_id
        return self.perform_sql_query_to_df(sql)

    def get_table(self, scheme_name, table_name, date_column_name="Date", from_date=str(date.today()),
                  to_date=str(date.today())):
        """
        Retrieves the wanted table from bigQuery as pandas df.
        Returns the rows from the current day (default behaviour) or filtered according to given time interval
        (including both sides).
        :param scheme_name: string represents the scheme name ("mrr", "dwh", etc.)
        :param table_name: string represents the table name.
        :param date_column_name: string represents the name of the column that holds the date information.
        :param from_date: string represents the interval's start date, format "YYYY-MM-DD"
        :param to_date: string represents the interval's end date, format "YYYY-MM-DD".
        :return: df with all the table's rows filtered be time.

        :raise TypeError when any arg is not a string
        :raise google.api_core.exception when error occurred.

        """
        check_types([scheme_name, table_name, date_column_name, from_date, to_date], [str, str, str, str, str])
        # increase date by one day since the second date in the between statement is interpreted as
        # midnight (when the day starts).
        to_date = increase_date_by_day(to_date)
        full_table_id = self.build_table_id(scheme_name, table_name)
        sql = """
                SELECT *
                FROM `%s`
                WHERE %s between "%s" and "%s"
            """ % (full_table_id, date_column_name, from_date, to_date)
        return self.perform_sql_query_to_df(sql)

    def get_newest_event_date(self, scheme_name, table_name, date_column_name="Date"):
        """
        :param scheme_name: string represents the scheme name ("mrr", "dwh", etc.)
        :param table_name: string represents the table name.
        :param date_column_name: string represents the name of the column that holds the date information
        :return: Type datetime.date, the date of the event with the last most date at the table

        :raise TypeError when incompatible args types
        :raise ValueError when projectId.scheme.tableName is not exist
        :raise other Exception when another error has occurred.
        """
        check_types([scheme_name, table_name, date_column_name], [str, str, str])
        full_table_id = self.build_table_id(scheme_name, table_name)
        self.assert_table_is_exist(full_table_id)
        sql = """
                SELECT max (%s) as Max_Date
                FROM %s
            """ % (date_column_name, full_table_id)
        df = self.perform_sql_query_to_df(sql)
        latest_update = df.iloc[0]["Max_Date"]
        return latest_update.date()

    def load_new_table(self, scheme_name, table_name, df):
        """
        Loads df to new table in bigQuery as: projectId.scheme.tableName.
        :param scheme_name: string represents the scheme name ("mrr", "dwh", etc.)
        :param table_name: string represents the table name.
        :param df: contains the new table's rows
        :return:

        :raise TypeError when incompatible args types
        :raise Conflict when projectId.scheme.tableName is already exists
        :raise NotFound when projectId.scheme is illegal.
        :raise other Exception when another error has occurred.
        """
        check_types([scheme_name, table_name, df], [str, str, pd.core.frame.DataFrame])
        self.load_table(self.build_table_id(scheme_name, table_name), df, "WRITE_EMPTY")

    def load_new_rows_to_exist_table(self, scheme_name, table_name, df):
        """
        Loads df as new rows in exist table.
        :param scheme_name: string represents the scheme name ("mrr", "dwh", etc.)
        :param table_name: string represents the table name.
        :param df: contains the new rows.
        :return:

        :raise TypeError when incompatible args types
        :raise ValueError when projectId.scheme.tableName is not exist
        :raise other Exception when another error has occurred.
        """
        check_types([scheme_name, table_name, df], [str, str, pd.core.frame.DataFrame])
        full_table_id = self.build_table_id(scheme_name, table_name)
        self.assert_table_is_exist(full_table_id)
        self.load_table(full_table_id, df, "WRITE_APPEND")

    def replace_rows_in_time_range(self, scheme_name, table_name, df, from_date, to_date, date_column_name="Date"):
        """
        Removes rows in the given interval [from_date, to_date] and appends the new rows to the exists table
        :param scheme_name: string represents the scheme name ("mrr", "dwh", etc.)
        :param table_name: string represents the table name.
        :param df: contains the new rows.
        :param from_date: string represents the deleted interval's start date, format "YYYY-MM-DD"
        :param to_date: string represents the deleted  interval's end date, format "YYYY-MM-DD".
        :param date_column_name: string represents the name of the column that holds the date information.
        :return:

        :raise TypeError when incompatible args types
        :raise ValueError when projectId.scheme.tableName is not exist
        :raise other Exception when another error has occurred.
        """
        check_types([scheme_name, table_name, df, from_date, to_date, date_column_name],
                    [str, str, pd.core.frame.DataFrame, str, str, str])
        full_table_id = self.build_table_id(scheme_name, table_name)
        self.assert_table_is_exist(full_table_id)
        # increase date by one day since the second date in the between statement is interpreted as
        # midnight (when the day starts).
        to_date = increase_date_by_day(to_date)
        sql = """
                DELETE `%s`
                WHERE %s between "%s" and "%s"
            """ % (full_table_id, date_column_name, from_date, to_date)
        self.perform_sql_query_to_df(sql)
        self.load_new_rows_to_exist_table(scheme_name, table_name, df)

    # def get_last_modified_date(self): //todo when we have "modified_date"

    # ------------------------------- inner methods (not as part of the API) ------------------------------------

    def load_table(self, full_table_id, df, write_config):
        job_config = bigquery.LoadJobConfig(write_disposition=write_config)
        try:
            job = self.client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
            job.result()
        except exceptions.Conflict:
            raise ValueError("Table " + full_table_id + " is already exists")
        except exceptions.NotFound:
            raise ValueError("Dataset " + full_table_id + " not found")
        except Exception as ex:
            raise ex

    def build_table_id(self, scheme_name, table_name):
        return self.db_name + "." + scheme_name + "." + table_name

    def perform_sql_query_to_df(self, sql_query):
        try:
            return self.client.query(sql_query).to_dataframe()
        except Exception as ex:
            raise ex

    def assert_table_is_exist(self, full_table_id):
        try:
            self.client.get_table(full_table_id)
        except exceptions.NotFound:
            raise ValueError("Table " + full_table_id + " is not exists")
