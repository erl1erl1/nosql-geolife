# QUERIES
import time
import copy
import pandas as pd
from database import DbConnector


class Part2:
    def __init__(self):
        """
        Inits part 1
        :param database: The Database object to operate on.
        """
        self.connector = DbConnector()
        self.client = self.connector.client
        self.db = self.connector.db
        self.user_collection = self.db['user']
        self.activity_collection = self.db['activity']
        self.tp_collection = self.db['trackpoint']

    def execute_tasks(self):
        self.sum_of_collections()
        self.avg_activities_per_user()
        self.top_20_users()
        self.users_taken_taxi()
        self.count_activites_with_transportation()
        self.year_with_most_activities()

    """
    Query 1: How many users, activities and trackpoints are there in the dataset (after it is inserted into the database).
    """

    def sum_of_collections(self):
        print("Query 1 - Total number of users, activites and trackpoints in the dataset: ")
        print(f"Total users: ", self.user_collection.count_documents({}))
        print(f"Total activities: ", self.activity_collection.count_documents({}))
        print(f"Total trackpoints: ", self.tp_collection.count_documents({}))

    """
    2. Find the average number of activities per user.
    """

    def avg_activities_per_user(self):
        sum_users = self.user_collection.count_documents({})
        sum_activities = self.activity_collection.count_documents({})
        avg_activities_per_user = sum_activities / sum_users
        print("Query 2 - Average activities per user:", f"{avg_activities_per_user:.2f}")

    """
    3. Find the top 20 users with the highest number of activities.
    """

    def top_20_users(self):
        # Aggregate data to get the count of activities per user, sort them, and limit to top 20
        pipeline = [
            {"$group": {"_id": "$user_id", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 20}
        ]
        result = list(self.activity_collection.aggregate(pipeline))

        # Print the result
        print("Query 3 - Top 20 users with the highest number of activities:")
        for rank, user in enumerate(result):
            print(f"Rank {rank + 1}: User {user['_id']} has {user['count']} activities")

    """
    4. Find all users who have taken a taxi.
    """

    def users_taken_taxi(self):
        pipeline = [
            {'$match': {'transportation_mode': 'taxi'}},
            {'$group': {'_id': '$user_id'}},
            {'$sort': {'_id': 1}}
        ]
        result = list(self.activity_collection.aggregate(pipeline))
        print(f"Query 4 - Users who have taken a taxi:")
        for user in result:
            print(f"User {user['_id']}")

    """
    5. Find all types of transportation modes and count how many activities that are
    tagged with these transportation mode labels. Do not count the rows where
    the mode is null.
    """

    def count_activites_with_transportation(self):
        pipeline = [
            {'$match': {'transportation_mode': {'$ne': ''}}},
            {'$group': {'_id': '$transportation_mode', 'count': {'$sum': 1}}},
            {'$sort': {'count': -1}}
        ]
        result = list(self.activity_collection.aggregate(pipeline))
        print(
            "Query 5: Find all types of transportation modes and count how many activities that are tagged with these transportation mode labels (except null transportation mode).")
        for row in result:
            print(f"{row['_id']}: {row['count']} ")

    """
    6. Find the year with the most activities. Is this also the year with most recorded hours?
    """
    def year_with_most_activities(self):
        # Query 6a
        pipeline = [{'$group': {'_id': {'$year': '$start_date_time'},
                                'count': {'$sum': 1}}},
                    {'$sort': {'count': -1}},
                    {'$limit': 1}]

        activity_result = list(self.activity_collection.aggregate(pipeline))
        activity_year = activity_result[0]['_id']
        activity_count = activity_result[0]['count']
        print(f"Most activities were recorded in {activity_year}, with {activity_count} activities")


        # Query 6b
        pipeline_2 = [
            {'$group': {'_id': {'$year': '$start_date_time'},
                        'sum': {
                            '$sum': {'$divide': [{'$subtract': ['$end_date_time', '$start_date_time']}, 1000 * 60 * 60]}
                        }
                        }
             },
            {'$sort': {'sum': -1}},
            {'$limit': 5}
        ]
        hours_result = list(self.activity_collection.aggregate(pipeline_2))
        hours_year = hours_result[0]['_id']
        hours_sum = hours_result[0]['sum']

        print(f"Most recorded hours were recorded in {hours_year} with {hours_sum:.2f} hours")

        if activity_year == hours_year:
            print(f"\nTherefore, {activity_year} was the year when most activities and hours were recorded.\n")
        else:
            print(f"\nThe activity when most activities were recorded was not the same as the year when most hours "
                  f"were recorded.")



    """
    7. Findthetotaldistance(inkm)walkedin2008,byuserwithid=112.
    """

    """
    8. Findthetop20userswhohavegainedthemostaltitudemeters.
        ○ Output should be a field with (id, total meters gained per user).
        ○ Remember that some altitude-values are invalid
        ○ Tip: ∑ (𝑡𝑝 𝑛. 𝑎𝑙𝑡𝑖𝑡𝑢𝑑𝑒 − 𝑡𝑝 𝑛−1. 𝑎𝑙𝑡𝑖𝑡𝑢𝑑𝑒), 𝑡𝑝 𝑛. 𝑎𝑙𝑡𝑖𝑡𝑢𝑑𝑒 > 𝑡𝑝 𝑛−1. 𝑎𝑙𝑡𝑖𝑡𝑢𝑑𝑒
    """

    """
    9. Findalluserswhohaveinvalidactivities,andthenumberofinvalidactivities per user
    ○ An invalid activity is defined as an activity with consecutive trackpoints where the timestamps deviate with at least 5 minutes.
    """

    """
    10.Find the users who have tracked an activity in the Forbidden City of Beijing. ○ In this question you can consider the Forbidden City to have
    coordinates that correspond to: lat 39.916, lon 116.397.
    """

    """
    11.Find all users who have registered transportation_mode and their most used
    transportation_mode.
    ○ The answer should be on format (user_id,
    most_used_transportation_mode) sorted on user_id.
    ○ Some users may have the same number of activities tagged with e.g.
    walk and car. In this case it is up to you to decide which transportation
    mode to include in your answer (choose one).
    ○ Do not count the rows where the mode is null.
    """
