import pandas as pd
import os


def read_file_to_list(file_path: str) -> list:
    """
    Reads a text file and returns each line as a string in a list.

    :param file_path: The path to the text file.
    :return: A list of strings, each representing a line in the text file.
    """
    try:
        with open(file_path, 'r') as file:
            lines_list = [line.strip() for line in file.readlines()]
        return lines_list
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def process_users(path: str, labeled_ids: list) -> list:
    """
    Processes user directories and returns a list of user data.

    :param path: The path to the user directories.
    :param labeled_ids: A list of labeled user IDs.
    :return: A list of dictionaries, each containing user data.
    """
    user_rows = []
    with os.scandir(path) as users:
        for user in users:
            if user.is_dir():
                user_row = {
                    "id": user.name,
                    "has_labels": user.name in labeled_ids,
                    "meta": {"path": user.path}
                }
                user_rows.append(user_row)
    return user_rows


def preprocess_activities(user_row: dict) -> list:
    """
    Processes activity files and returns a list of activity data.

    :param user_row: A dictionary containing user data.
    :return: A list of dictionaries, each containing activity data.
    """
    activity_rows = []
    with os.scandir(user_row['meta']['path'] + "/Trajectory") as activities:
        for activity in activities:
            if activity.is_file():
                activity_row = {
                    "id": int(activity.name[:-4] + user_row["id"]),
                    "user_id": user_row["id"],
                    "transportation_mode": None,
                    "meta": {"path": activity.path}
                }
                activity_rows.append(activity_row)
    return activity_rows


def process_activity(user_row: dict, activity_row: dict) -> tuple:
    """
    Processes an activity and returns the expanded activity data and trackpoints data frame.

    :param user_row: A dictionary containing user data.
    :param activity_row: A dictionary containing activity data.
    :return: A tuple containing the expanded activity data and trackpoints data frame.
    """
    columns = ['lat', 'lon', 'dep1', 'alt', 'date', 'date_str', 'time_str']
    trackpoints_df = pd.read_table(activity_row['meta']['path'], skiprows=6, names=columns, delimiter=',')

    if trackpoints_df.shape[0] > 2500:
        return None, None

    activity_row['start_date_time'] = pd.to_datetime(
        trackpoints_df['date_str'].iloc[0] + " " + trackpoints_df['time_str'].iloc[0])
    activity_row['end_date_time'] = pd.to_datetime(
        trackpoints_df['date_str'].iloc[-1] + " " + trackpoints_df['time_str'].iloc[-1])

    if user_row['has_labels']:
        transportations = pd.read_table(user_row['meta']['path'] + "/labels.txt")
        transportations['Start Time'] = pd.to_datetime(transportations['Start Time'])
        transportations['End Time'] = pd.to_datetime(transportations['End Time'])

        time_tolerance = pd.Timedelta(seconds=0)
        matching_transport = transportations[
            (transportations['Start Time'].between(activity_row['start_date_time'] - time_tolerance,
                                                   activity_row['start_date_time'] + time_tolerance)) &
            (transportations['End Time'].between(activity_row['end_date_time'] - time_tolerance,
                                                 activity_row['end_date_time'] + time_tolerance))
            ]

        if not matching_transport.empty:
            activity_row['transportation_mode'] = matching_transport['Transportation Mode'].iloc[0]

    return activity_row, trackpoints_df


def process_trackpoint(activity_id: int, trackpoint_row: pd.Series) -> dict:
    """
    Processes a trackpoint and returns the trackpoint data.

    :param activity_id: The ID of the activity.
    :param trackpoint_row: A pandas Series containing trackpoint data.
    :return: A dictionary containing the processed trackpoint data.
    """
    return {
        'activity_id': activity_id,
        'lat': trackpoint_row['lat'],
        'lon': trackpoint_row['lon'],
        'altitude': trackpoint_row['alt'] if trackpoint_row['alt'] != -777 else None,
        'date_days': trackpoint_row['date'],
        'date_time': trackpoint_row['date_str'] + " " + trackpoint_row['time_str']
    }
