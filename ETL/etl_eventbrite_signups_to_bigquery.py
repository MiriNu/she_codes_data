from etl_directory.BigqueryConnector import BigqueryConnector
from etl_directory.EventbriteConnector import EventbriteConnector
import pandas as pd


def etl_events_table(b, e):
    """
    Updates the "eventbrite_events" table to contain also newer events-
    events that occurred between the newest event in the table and today.
    :param b: BigquaryConnector
    :param e: EventbriteConnector
    :return:
    """
    schema = "workspace" # todo: should be the mrr
    table_name = "eventsbrite_events_copy" # todo: not copy
    latest_date = b.get_newest_event_date(schema, table_name)
    new_events = e.get_arranged_events_in_time_range(str(latest_date))
    # replace the rows in order to hold all the events that occurred on the latest date, and only once.
    b.replace_rows_in_time_range(schema, table_name, new_events, str(latest_date), str(latest_date), "Date")


def etl_signups_table(b, e):
    """
    Updates the "eventbrite_signups" table to contain also newer signups-
    signups of events that occurred between the newest signup record in the table and today.
    :param b: BigquaryConnector
    :param e: EventbriteConnector
    :return:
    """
    # Right now the date col in the signups table is in inappropriate format, should be changes.
    # For now, you can create "dec_signups" by the func "db_create_formatted_signups_table" and check
    # your work on it.
    schema = "workspace" # todo: should be the dwh
    table_name = "dec_signups" # todo: eventbrite_signups
    latest_date = b.get_newest_event_date(schema, table_name)
    new_events = e.get_arranged_signups_in_time_range(str(latest_date))
    b.replace_rows_in_time_range(schema, table_name, new_events, str(latest_date), str(latest_date), "Date")

# ------------------------------------------- db only ---------------------------------------------------


def db_create_formatted_signups_table():
    dec_signups = e.get_arranged_signups_in_time_range("2021-12-01", "2021-12-31")
    b.load_new_table("workspace", "dec_signups", dec_signups)


def db_copy_events_table_to_workspace(b):
    b.load_new_table("workspace", "eventsbrite_events_copy", b.get_dim_full_table("mrr", "eventbrite_events"))


def db_copy_signups_table_to_workspace(b):
    b.load_new_table("workspace", "eventbrite_signups_copy", b.get_dim_full_table("dwh", "eventbrite_signups"))


if __name__ == '__main__':
    b = BigqueryConnector(
        "C:\\Users\\naama\\Documents\\sheCodes\\firstTask\\etl_directory\\concrete-bloom-330808-fe99bbc7ff24.json")
    e = EventbriteConnector("C:\\Users\\naama\\Documents\\sheCodes\\firstTask\\etl_directory\\eventbrite-info.json")
    pd.set_option('display.max_columns', None, 'display.max_rows', None, 'expand_frame_repr', False,'display.max_colwidth', None)
    etl_events_table(b, e)
    etl_signups_table(b, e)
    print("end")

