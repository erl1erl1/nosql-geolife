import time
import copy
import pandas as pd
from database import DbConnector
from data_processing import (process_users, preprocess_activities, process_activity, process_trackpoint,
                             read_file_to_list)
from helpers import time_elapsed_str


class Part1:
    def __init__(self):
        """
        Inits part 1
        :param database: The Database object to operate on.
        """
        self.connector = DbConnector()
        self.client = self.connector.client
        self.database = self.connector.db
        self.user_collection = self.database['user']
        self.activity_collection = self.database['activity']
        self.tp_collection = self.database['trackpoint']

    def push_buffers_to_db(self, activity_buffer, trackpoint_buffer, num_activities, num_trackpoints):
        """
        Push processed activities and trackpoints to the database.

        :param activity_buffer: A list of buffered activities.
        :param trackpoint_buffer: A list of buffered trackpoints.
        :param num_activities: The number of activities.
        :param num_trackpoints: The number of trackpoints.
        """
        insert_time = time.time()
        print(f'\nInserting: {num_activities} activities and {num_trackpoints} trackpoints')

        # Insert activities
        self.activity_collection.insert_many(activity_buffer)
        activity_buffer.clear()

        # Insert trackpoints
        self.tp_collection.insert_many(trackpoint_buffer)
        trackpoint_buffer.clear()

        print(f'\tInsertion time: {time_elapsed_str(insert_time)}\n'
              f'\tInserts per second: {int((num_trackpoints + num_activities) / (time.time() - insert_time))}\n')

    def insert_data(self, data_path, labeled_ids, insert_threshold=10e4):
        """
        Insert data into the database.

        :param data_path: The path to the data to be inserted.
        :param labeled_ids: A list of labeled IDs.
        :param insert_threshold: The threshold for batch insertion.
        """
        start_time = time.time()
        users_rows = process_users(path=data_path, labeled_ids=labeled_ids)
        self.user_collection.insert_many(copy.deepcopy(users_rows))
        num_users = len(users_rows)
        print(f"Inserted {num_users} users into User\n")

        activity_buffer = []
        trackpoint_buffer = []

        for i, user_row in enumerate(users_rows):
            activity_rows = preprocess_activities(user_row=user_row)

            for activity_row in activity_rows:
                activity, trackpoints_df = process_activity(user_row, activity_row=activity_row)
                if not activity:  # means number of trackpoints > 2500
                    continue

                activity_buffer.append(activity)

                for _, trackpoint_row in trackpoints_df.iterrows():
                    trackpoint = process_trackpoint(activity['id'], trackpoint_row)
                    trackpoint_buffer.append(trackpoint)

                num_activities, num_trackpoints = len(activity_buffer), len(trackpoint_buffer)
                if num_activities + num_trackpoints > insert_threshold:
                    self.push_buffers_to_db(activity_buffer, trackpoint_buffer, num_activities, num_trackpoints)

            print(
                f'\rUser {user_row["id"]} processed ({i + 1} / {num_users}), Time elapsed: {time_elapsed_str(start_time)}',
                end='')

        self.push_buffers_to_db(activity_buffer, trackpoint_buffer, len(activity_buffer), len(trackpoint_buffer))
        print(f'\nInsertion complete - Total time: {time_elapsed_str(start_time)}')

    def upload_data(self):
        """
        Execute the database operations.
        """
        data_path = './dataset/dataset/Data'
        labeled_ids = read_file_to_list('./dataset/dataset/labeled_ids.txt')
        self.insert_data(data_path, labeled_ids, insert_threshold=325 * 10e2)
        #self.drop_collections()
        self.connector.close_connection()

    def drop_collections(self):
        self.user_collection.drop()
        self.activity_collection.drop()
        self.tp_collection.drop()