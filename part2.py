# QUERIES
import time
import copy
import pandas as pd
from tabulate import tabulate
from datetime import datetime
from haversine import haversine

from database import DbConnector
def print_question(task_num: int, question_text: str):
    """
    Prints task introduction.
    Args:
        task_num: number of the task
        question_text: Text to display
    """
    print(f'Task {task_num}:')
    print(question_text)
    print("Querying... Please wait.", end='')


def print_result(result_df: pd.DataFrame or dict[str, list], floatfmt=".0f", filename=None):
    """
    Tabulates and prints the result table of a query
    Args:
        result_df: result table from query as Dataframe or dictionary of lists
        floatfmt: decimal precision
        filename: name of file to write the result table to. Omit to avoid writing to file.

    Returns:

    """
    print('\r', end='')

    display = tabulate(result_df, headers='keys', tablefmt='grid', floatfmt=floatfmt, showindex=False)
    print(display + "\n")

    # Write to file if given
    if filename:
        with open(f'task_outputs/{filename}.txt', 'w') as f:
            f.write(display)

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

    def execute_tasks(self, task_nums: int or list[int]):
        """
            Executes specified tasks based on provided task numbers.

            :param task_nums: An integer, range, or list of integers representing the task numbers
                              to be executed.
            """
        tasks = [self.sum_of_collections, self.avg_activities_per_user, self.top_20_users,
                 self.users_taken_taxi, self.count_activites_with_transportation, self.year_with_most_activities,
                 self.km_walked_in_2008_by_user_112, self.top_20_users_with_most_altitude_meters,
                 self.users_with_invalid_activities, self.users_with_activity_in_beijing,
                 self.user_transportation_mode]

        if isinstance(task_nums, int):
            task_nums = [task_nums]

        for num in task_nums:
            tasks[num - 1]()

    """
    Query 1: How many users, activities and trackpoints are there in the dataset (after it is inserted into the database).
    """

    def sum_of_collections(self):
        print_question(task_num=1,
                       question_text="How many users, activities and trackpoints are there in the dataset "
                                     "(after it is inserted into the database)?")

        result = {'Number of Users': [self.user_collection.count_documents({})],
                  'Number of Activities': [self.activity_collection.count_documents({})],
                  'Number of TrackPoints': [self.tp_collection.count_documents({})]}

        print_result(result_df=result, filename=f"task_{1}")

    """
    2. Find the average number of activities per user.
    """

    def avg_activities_per_user(self):
        print_question(task_num=2, question_text="Find the average number of activities per user.")

        # Count total users
        total_users = self.user_collection.count_documents({})

        user_pipeline = [
            {'$lookup': {
                'from': 'activity',
                'localField': 'id',
                'foreignField': 'user_id',
                'as': 'activities'
            }},
            {'$project': {
                'activity_count': {'$size': '$activities'}
            }}
        ]
        users_activities = list(self.user_collection.aggregate(user_pipeline))

        # Calculate average activities per user
        total_activities = sum(user_activity['activity_count'] for user_activity in users_activities)

        if total_users > 0:
            avg_activities_per_user = total_activities / total_users
        else:
            avg_activities_per_user = 0
        
        result = {"Average activities per user": [avg_activities_per_user]}

        print_result(result_df=result, filename=f"task_2")

    """
    3. Find the top 20 users with the highest number of activities.
    """

    def top_20_users(self):
        print_question(task_num=3, question_text="Top 20 users with the highest number of activities.")
        # Aggregate data to get the count of activities per user, sort them, and limit to top 20
        pipeline = [
            {"$group": {"_id": "$user_id", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 20}
        ]
        result = list(self.activity_collection.aggregate(pipeline))

        df = pd.DataFrame(result)
        df.rename({"_id": "User ID", "count":"Count"}, inplace=True, axis=1)
        print("")
        print_result(result_df=df, filename=f"task_3")

    """
    4. Find all users who have taken a taxi.
    """

    def users_taken_taxi(self):
        print_question(task_num=4, question_text="Find all users who have taken a taxi.")
        pipeline = [
            {'$match': {'transportation_mode': 'taxi'}},
            {'$group': {'_id': '$user_id'}},
            {'$sort': {'_id': 1}}
        ]
        result = list(self.activity_collection.aggregate(pipeline))
        print(f"Query 4 - Users who have taken a taxi:")
        df = pd.DataFrame(result)
        df.rename({"_id": "User ID"}, inplace=True, axis=1)
        print_result(result_df=df, filename="task_4")

    """
    5. Find all types of transportation modes and count how many activities that are
    tagged with these transportation mode labels. Do not count the rows where
    the mode is null.
    """

    def count_activites_with_transportation(self):
        pipeline = [
            {'$match': {'transportation_mode': {'$ne': None}}},
            {'$group': {'_id': '$transportation_mode', 'count': {'$sum': 1}}},
            {'$sort': {'count': -1}}
        ]
        result = list(self.activity_collection.aggregate(pipeline))
        print_question(task_num=5,
            question_text="All types of transportation modes and the number of activities that are tagged with thesetransportation mode labels (except null transportation mode).")
        for row in result:
            print(f"{row['_id']}: {row['count']} ")
        df = pd.DataFrame(result)
        df.rename({"_id": "Transportation Mode", "count": "Count"}, inplace=True, axis=1)
        print_result(result_df=df, filename="task_5")

    """
    6. Find the year with the most activities. Is this also the year with most recorded hours?
    """

    def year_with_most_activities(self):
        print_question(task_num=6, question_text=f"A: Most activities recorded in a year.")
        # Query 6a
        pipeline = [{'$group': {'_id': {'$year': '$start_date_time'},
                                'count': {'$sum': 1}}},
                    {'$sort': {'count': -1}},
                    {'$limit': 1}]

        activity_result = list(self.activity_collection.aggregate(pipeline))
        activity_year = activity_result[0]['_id']
        activity_count = activity_result[0]['count']
        result = {
            "Year": [activity_year],
            "Activities": [activity_count]
        }
        print("")
        print_result(result_df=result, filename="task_6a")
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

        print_question(task_num=6, question_text=f"B: Most recorded hours were recorded in hours.")

        result = {
            "Year": [hours_year],
            "Hours": [hours_sum]
        }
        print("")
        print_result(result_df=result, filename="6b")

        if activity_year == hours_year:
            print(f"\nTherefore, {activity_year} was the year when most activities and hours were recorded.\n")
        else:
            print(f"\nThe year when most activities were recorded was not the same as the year when most hours "
                  f"were recorded.")

    """
    7. Find the total distance (in km) walked in 2008, by user with id = 112.
    """

    def km_walked_in_2008_by_user_112(self):
        # Retrieve all activity-id's labeled 'walk' for user 112 in 2008
        query = {"user_id": "112",
                 "transportation_mode": "walk"}
        projection = {"_id": 0,
                  "id": 1}

        activities_result = self.activity_collection.find(query, projection)

        activities_list = []
        for activity in activities_result:
            activities_list.append(activity['id'])

        # Retrieve trackpoints for each activity id
        pipeline_2 = ({
                          "activity_id": {"$in": activities_list}},
                      {
                          "_id": False,
                          "id": True,
                          "activity_id": True,
                          "lat": True,
                          "lon": True
                      })

        trackpoints_list = list(self.tp_collection.find(pipeline_2[0], pipeline_2[1]))

        distance_in_km = 0
        # Calculate the distance between each trackpoint
        for i in range(len(trackpoints_list) - 1):
            activity = trackpoints_list[i]['activity_id']
            next_activity = trackpoints_list[i + 1]['activity_id']

            # Only calculate trackpoints in the same activity
            if activity != next_activity:
                continue

            lat_1 = trackpoints_list[i]['lat']
            lon_1 = trackpoints_list[i]['lon']
            lat_2 = trackpoints_list[i + 1]['lat']
            lon_2 = trackpoints_list[i + 1]['lon']

            # Calculate distance between the two points with haversine
            distance_in_km += haversine((lat_1, lon_1), (lat_2, lon_2))

        print("Query 7 - the total distance (in km) walked in 2008, by user with id = 112:", "\n")
        print(f"{distance_in_km:.2f}")

    """
    8. Find the top 20 users who have gained the most altitude meters.
        ○ Output should be a field with (id, total meters gained per user).
        ○ Remember that some altitude-values are invalid
        ○ Tip: ∑ (𝑡𝑝 𝑛. 𝑎𝑙𝑡𝑖𝑡𝑢𝑑𝑒 − 𝑡𝑝 𝑛−1. 𝑎𝑙𝑡𝑖𝑡𝑢𝑑𝑒), 𝑡𝑝 𝑛. 𝑎𝑙𝑡𝑖𝑡𝑢𝑑𝑒 > 𝑡𝑝 𝑛−1. 𝑎𝑙𝑡𝑖𝑡𝑢𝑑𝑒
    """

    def top_20_users_with_most_altitude_meters(self):
        pipeline = {
            '_id': False,
            'user_id': True,
            'activity_id': True,
            'altitude': True
        }
        result = self.tp_collection.find({}, pipeline)

        trackpoint_alt = list(result)

        user_alt = dict()

        # Calculate gained altitude for every user
        for index in range(len(trackpoint_alt)):
            # Break if end of trackpoints
            if index == len(trackpoint_alt) - 1:
                break

            user_id = trackpoint_alt[index]['user_id']
            activity_id_1 = trackpoint_alt[index]['activity_id']
            activity_id_2 = trackpoint_alt[index + 1]['activity_id']

            # Only calculate altitudes for trackpoints within the same activity
            if activity_id_1 != activity_id_2:
                continue

            # Initialize dictionary
            if user_id not in user_alt:
                user_alt[user_id] = 0

            alt_1 = trackpoint_alt[index]['altitude']
            alt_2 = trackpoint_alt[index + 1]['altitude']

            # Only include valid altitudes
            if not alt_1 or not alt_2:
                continue

            delta_alt = alt_2 - alt_1
            user_alt[user_id] += delta_alt

        # Sorting dictionary
        user_alt_array = sorted(
            user_alt.items(), key=lambda x: x[1], reverse=True)

        results = []

        for i, (user_id, alt) in enumerate(user_alt_array[:20]):
            results.append([i + 1, user_id, round(alt)])

        print(f"Query 8 - Top 20 users who have gained the most altitude meters:")
        print(results)

    """
    9. Find all users who have invalid activities,and the number of invalid activities per user
    ○ An invalid activity is defined as an activity with consecutive trackpoints where the timestamps deviate with at least 5 minutes.
    """

    def users_with_invalid_activities(self):
        pipeline = {
            '_id': False,
            'user_id': True,
            'activity_id': True,
            'date_time': True
        }

        result = self.tp_collection.find({}, pipeline)
        trackpoints = list(result)
        invalid_user_activities = dict()

        # Find invalid trackpoints
        for index in range(len(trackpoints)):
            if index == len(trackpoints) - 1:
                break
            user_id = trackpoints[index]['user_id']
            activity_id_1 = trackpoints[index]['activity_id']
            activity_id_2 = trackpoints[index + 1]['activity_id']

            # Only compare trackpoints within the same activity
            if activity_id_1 != activity_id_2:
                continue

            # Initialize dictionary
            if user_id not in invalid_user_activities:
                invalid_user_activities[user_id] = set()

            date_time_1 = trackpoints[index]['date_time']
            date_time_2 = trackpoints[index + 1]['date_time']
            
            date_time_1 = datetime.strptime(date_time_1, '%Y-%m-%d %H:%M:%S')
            date_time_2 = datetime.strptime(date_time_2, '%Y-%m-%d %H:%M:%S')

            delta_date_time = date_time_2 - date_time_1
            if delta_date_time.seconds > 60 * 5:
                invalid_user_activities[user_id].add(activity_id_1)

        # Sort dictionary
        invalid_activities = sorted(invalid_user_activities.items())

        results = []

        for user_id, activities in invalid_activities:
            results.append([user_id, len(activities)])

        print("Query 9 - Users with invalid activities and the number of invalid activities:")
        print(results)

    """
    10.Find the users who have tracked an activity in the Forbidden City of Beijing. ○ In this question you can consider the Forbidden City to have
    coordinates that correspond to: lat 39.916, lon 116.397.
    """

    def users_with_activity_in_beijing(self):
        pipeline = [{'$match': {
            'lat': {
                '$gte': 39.916,
                '$lte': 39.917},
            'lon': {
                '$gte': 116.397,
                '$lte': 116.398
            }
        }
        }, {'$group': {
            '_id': '$user_id',
        }}]

        result = self.tp_collection.aggregate(pipeline)

        results = []

        for row in list(result):
            results.append(f"User {row['_id']} has trackpoints in the forbidden city\n")

        print("Query 10 - Users with tracked activity in the forbidden city Beijing:")
        print(results)

    """
    11.Find all users who have registered transportation_mode and their most used transportation_mode.
    ○ The answer should be on format (user_id, most_used_transportation_mode) sorted on user_id.
    ○ Some users may have the same number of activities tagged with e.g.
    walk and car. In this case it is up to you to decide which transportation
    mode to include in your answer (choose one).
    ○ Do not count the rows where the mode is null.
    """

    def user_transportation_mode(self):
        pipeline = {
            'transportation_mode': {
                '$ne': None
            }
        }, {
            '_id': False,
            'user_id': True,
            'transportation_mode': True
        }
        result = self.activity_collection.find(pipeline[0], pipeline[1])

        activities = list(result)
        #print(activities)
        user_mode = dict()

        for activity in activities:
            user_id = activity['user_id']
            transportation_mode = activity['transportation_mode']

            # Initialize user_mode dictionary
            if user_id not in user_mode:
                user_mode[user_id] = dict()

            # Initialize transportation_mode dictionary
            if transportation_mode not in user_mode[user_id]:
                user_mode[user_id][transportation_mode] = 0

            # Count each transportation mode
            #user_mode[user_id][transportation_mode] += 1

        # Sort the dictionary
        user_mode_sorted = sorted(user_mode.items())

        results = []

        for user_id, transportation_modes in user_mode_sorted:
            transportation_mode = max(transportation_modes, key=transportation_modes.get)
            results.append([user_id, transportation_mode])

        print("Query 11 - Users with transportation mode registered and their most used transportation mode:")
        print(results)
