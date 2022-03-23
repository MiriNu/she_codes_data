import json
import requests
import pandas as pd
from datetime import date


def get_request(url):
    response = requests.request("GET", url).json()
    return response


def check_types(arg_list, expected_types):
    if len(arg_list) != len(expected_types):
        raise ValueError("The expected type array length does not match the list to check length")
    for i in range(len(arg_list)):
        if type(arg_list[i]) != expected_types[i]:
            raise TypeError("Illegal type, expected types are:" + str(expected_types))


class EventbriteConnector:

    def __init__(self, json_path):
        json_file = open(json_path)
        she_codes_parameters = json.load(json_file)
        json_file.close()
        self.token = she_codes_parameters["token"]
        self.organization_id = she_codes_parameters["organization_id"]

    def get_arranged_events_in_time_range(self, from_date, to_date=str(date.today())):
        """
        Returns all the events that their start date is in the interval [from_date, to_date].
        :param from_date: string represents the interval's start date, format "YYYY-MM-DD"
        :param to_date: string represents the interval's end date, format "YYYY-MM-DD".
        :return: df that contains all the events in the time range, according to the table schema.
        """
        check_types([from_date, to_date], [str, str])
        return self.project_into_events_table_schema(self.get_events_in_time_range(from_date, to_date))

    def get_arranged_signups_in_time_range(self, from_date, to_date=str(date.today())):
        """
        Returns all the signups of events that their start date is in the interval [from_date, to_date].
        :param from_date: string represents the interval's start date, format "YYYY-MM-DD"
        :param to_date: string represents the interval's end date, format "YYYY-MM-DD".
        :return: df that contains all the signups of events in the time range, according to the table schema.
        """
        check_types([from_date, to_date], [str, str])
        events_in_time_range = self.get_events_in_time_range(from_date, to_date)
        signups_per_event_list = []
        # we iterates over all the new events collecting the new signups
        for index, row in events_in_time_range.iterrows():
            signups_per_event_list.append(
                self.get_event_signups_according_to_signups_table_scheme(row['id'], row['name.text'], row['start.utc']))
        large_df = pd.concat(signups_per_event_list, ignore_index=True)
        return large_df

    # ------------------------------- inner methods (not as part of the API) ------------------------------------

    def get_events_in_time_range(self, from_date, to_date):
        get_command = "https://www.eventbriteapi.com/v3/organizations/" \
                      + self.organization_id + "/events/?&token=" \
                      + self.token + "&expand=ticket_classes&time_filter=past&start_date.range_start=" \
                      + from_date + "&start_date.range_end=" \
                      + to_date
        # Since we get the answer in paging way we call "get_all_rows"
        all_information_df = self.get_all_rows(get_command, get_request(get_command), "events")
        return all_information_df

    def project_into_events_table_schema(self, all_information_df):
        new_table_data = [{"Event": row['name.text'],
                           "Date": pd.to_datetime(row['start.local']),
                           "ToDate": pd.to_datetime(row['end.local']),
                           "Status": row['status'],
                           "Tickets_Sold": row['ticket_classes'][0]['quantity_sold'],
                           "Tickets_Available": max(0, row['ticket_classes'][0]['quantity_total']
                                                    - row['ticket_classes'][0]['quantity_sold']),
                           "Subject": None,
                           "Category": None,
                           # todo: change after schema get changed (Will be added soon, preparation)
                           # "Subject_id": None,
                           # "Category_id": None,
                           # "Event_id": row['ticket_classes'][0]['event_id'],
                           # "Url": row['url']
                           } for index, row in all_information_df.iterrows()]
        eventbrite_df = pd.DataFrame(new_table_data)
        return eventbrite_df

    def get_event_signups(self, event_id):
        get_command = "https://www.eventbriteapi.com/v3/events/" + event_id + "/attendees/?&token=" + self.token
        # Since we get the answer in paging way we call "get_all_rows"
        all_signups_df = self.get_all_rows(get_command, get_request(get_command), "attendees")
        return all_signups_df

    def get_event_signups_according_to_signups_table_scheme(self, event_id, event_name, date):
        # todo: improve the way we retrieve the answers.
        # todo: take the col name and the answer to be a map
        all_signups_data = self.get_event_signups(event_id)
        new_table_data = [{"Are_you_a_she_codes__team_member_": None,
                           "Are_you_a_student_": self.get_boolean_answer(row["answers"], "Are you  a student?"),
                           "Are_you_currently_a_participant_at_she_codes__": self.get_boolean_answer(row["answers"],
                                                                                                     "Are you currently a participant as she codes;? (Study one of the courses?)"),
                           "Are_you_currently_employed_": self.get_boolean_answer(row["answers"],
                                                                                  "Are you currently employed? "),
                           "Are_you_having_a_hard_time_finding_a_job_in_high_tech_following_the_Corona_crisis_": None,
                           "Are_you_looking_for_a_job_in_a_technological_field_": self.get_answer(row["answers"],
                                                                                                  "Are you looking for a job in a technological field? "),
                           "Company": self.get_answer(row['answers'], "Company Name"),
                           "Corona_": self.calculate_corona(row),
                           "Date": pd.to_datetime(date),
                           "Do_you_have_a_degree_in_any_technology_related_field_": None,
                           "Do_you_have_an_academic_degree__if_yes__what_is_your_graduation_year_": self.get_answer(
                               row["answers"], "Do you have an academic degree? if yes, what is your graduation year?"),
                           "Do_you_have_any_work_experience_in_a_technology_field_": self.get_boolean_answer(
                               row["answers"], "Do you have any work experience in a technology field?"),
                           "Do_you_plan_on_beginning_your_academic_studies_this_year_": None,
                           "Email": None,
                           "Email_address_with_which_you_sign_up_for_shecodes_": row['profile.email'],
                           "Event_ID": int(event_id),
                           "Event_Name": event_name,
                           "From_which_branch_": self.get_answer(row["answers"], "From which branch?"),
                           "Has_your_scope_of_work_or_your_salary_been_reduced_as_a_result_of_the_corona_crisis_": self.get_answer(
                               row["answers"],
                               "Has your scope of work or your salary been reduced as a result of the corona crisis?"),
                           "How_many_years_of_experience_do_you_have_": self.get_answer(row["answers"],
                                                                                        "How many years of experience do you have?"),
                           "I_agree_to_receive_messages_by_Email": self.get_boolean_answer(row["answers"],
                                                                                           "I agree to receive messages by Email"),
                           "I_agree_to_recieve_messages_by_SMS": self.get_boolean_answer(row["answers"],
                                                                                         "I agree to receive messages by SMS"),
                           "ID_Number": None,
                           "Job_Title": self.get_answer(row['answers'], "Job Title"),
                           "Order_Date": pd.to_datetime(row['created']),
                           "Start_Date__month___year__or_year_only_": None,
                           "Ticket_Type": row['ticket_class_id'],
                           "What_degree_do_you_study_": None,
                           "What_field_do_you_intend_to_study_": None,
                           "What_is_your_estimated_graduation_year_": self.get_answer(row["answers"],
                                                                                      "What is your estimated graduation year? "),
                           "Which_lesson_are_you_at_": self.get_answer(row['answers'], "Which lesson are you at?"),
                           "Your_track_at_she_codes_": self.get_answer(row['answers'], "Which Track do you study?"),
                           } for index, row in all_signups_data.iterrows()]

        event_signups_df = pd.DataFrame(new_table_data)
        return event_signups_df

    def get_answer(self, all_answers, question):
        for q in all_answers:
            if q['question'] == question and 'answer' in q:
                return q['answer']
        return None

    def get_boolean_answer(self, all_answers, question):
        answer = self.get_answer(all_answers, question)
        if answer == "Yes":
            return True
        if answer == "No":
            return False
        return None

    def get_all_rows(self, basic_get_command, json, tag_as_string):
        """
        Iterates over the pages and collect the full response (according to the given tag) into one df.
        :param basic_get_command: The get command.
        :param json: the first answer.
        :param tag_as_string: the tag we interest in.
        :return: df with all the rows, over all the pages.
        """
        all_information_df = pd.json_normalize(json[tag_as_string])
        while json["pagination"]["has_more_items"]:
            continuation_id = json["pagination"]["continuation"]
            json = get_request(basic_get_command + "&continuation=" + continuation_id)
            page_df = pd.json_normalize(json[tag_as_string])
            all_information_df = pd.concat([all_information_df, page_df], ignore_index=True)
        return all_information_df

    def calculate_corona(self, row):
        student_answer = self.get_boolean_answer(row["answers"], "Are you  a student?")
        degree_with_graduation_answer = self.get_answer(row["answers"],
                                                        "Do you have an academic degree? if yes, what is your graduation year?")
        estimated_graduation_answer = self.get_answer(row["answers"], "What is your estimated graduation year? ")
        if ((student_answer is True and estimated_graduation_answer == '2021')
                or (student_answer is False and degree_with_graduation_answer in ['2019', '2020', '2021'])):
            return True

        currently_employed_answer = self.get_boolean_answer(row["answers"], "Are you currently employed? ")
        affected_by_corona_answer = self.get_answer(row["answers"],
                                                    "Has your scope of work or your salary been reduced as a result of the corona crisis?")
        if (currently_employed_answer is False or (
                currently_employed_answer is True and affected_by_corona_answer == "Yes")):
            return True
        return False
