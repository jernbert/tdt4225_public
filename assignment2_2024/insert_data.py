import os
from DbConnector import DbConnector
from datetime import datetime
import mysql.connector as mysql


class DataInserter:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
        self.valid_transportation_modes = {
            'walk', 'bike', 'bus', 'taxi', 'car',
            'subway', 'train', 'airplane', 'boat',
            'run', 'motorcycle'
        }

    def create_tables(self):
        # Create User table
        create_user_table = """
        CREATE TABLE IF NOT EXISTS User (
            id INT NOT NULL,
            has_labels BOOLEAN NOT NULL,
            PRIMARY KEY (id)
        );
        """
        self.cursor.execute(create_user_table)

        # Create Activity table
        create_activity_table = """
        CREATE TABLE IF NOT EXISTS Activity (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            transportation_mode VARCHAR(50),
            start_date_time DATETIME NOT NULL,
            end_date_time DATETIME NOT NULL,
            FOREIGN KEY (user_id) REFERENCES User(id) ON DELETE CASCADE
        );
        """
        self.cursor.execute(create_activity_table)

        # Create TrackPoint table
        create_trackpoint_table = """
        CREATE TABLE IF NOT EXISTS TrackPoint (
            id INT AUTO_INCREMENT PRIMARY KEY,
            activity_id INT NOT NULL,
            lat DOUBLE NOT NULL,
            lon DOUBLE NOT NULL,
            altitude FLOAT,
            date_days DOUBLE,
            date_time DATETIME NOT NULL,
            FOREIGN KEY (activity_id) REFERENCES Activity(id) ON DELETE CASCADE
        );
        """
        self.cursor.execute(create_trackpoint_table)
        self.db_connection.commit()

    def insert_data(self, data_path):
        # Adjust data_path because of nested 'dataset' directory
        data_path = os.path.join(data_path, 'dataset')
        # Read labeled_ids.txt to get users with labels
        labeled_user_ids = self.get_labeled_user_ids(data_path)

        users_path = os.path.join(data_path, 'Data')

        user_folders = [f for f in os.listdir(users_path) if os.path.isdir(os.path.join(users_path, f))]
        total_users = len(user_folders)
        print(f"Total users: {total_users}")

        for user_folder in user_folders:
            user_id = int(user_folder)
            user_path = os.path.join(users_path, user_folder)
            has_labels = user_id in labeled_user_ids

            # Insert user into User table
            self.cursor.execute("INSERT INTO User (id, has_labels) VALUES (%s, %s)", (user_id, has_labels))
            self.db_connection.commit()

            labels = []
            if has_labels:
                labels = self.read_labels(user_path)

            # Process the activities
            trajectory_path = os.path.join(user_path, 'Trajectory')
            if not os.path.exists(trajectory_path):
                continue  # Skip if Trajectory folder does not exist
            trajectory_files = [f for f in os.listdir(trajectory_path) if f.endswith('.plt')]
            for trajectory_file in trajectory_files:
                trajectory_file_path = os.path.join(trajectory_path, trajectory_file)
                trackpoints = self.read_trajectory_file(trajectory_file_path)
                if len(trackpoints) > 2500:
                    continue  # Ignore this activity

                # Determine the start and end times
                start_date_time = trackpoints[0]['date_time']
                end_date_time = trackpoints[-1]['date_time']

                transportation_mode = None

                if has_labels:
                    # Try to find an exact match in labels
                    transportation_mode = self.get_transportation_mode(labels, start_date_time, end_date_time)
                    # If transportation_mode is invalid, set it to None
                    if transportation_mode and transportation_mode not in self.valid_transportation_modes:
                        transportation_mode = None
                else:
                    # For users without labels, insert the activity with transportation_mode as NULL
                    pass

                # Insert the activity into Activity table
                self.cursor.execute(
                    "INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) VALUES (%s, %s, %s, %s)",
                    (user_id, transportation_mode, start_date_time, end_date_time))
                activity_id = self.cursor.lastrowid

                # Now batch insert the trackpoints
                self.insert_trackpoints(activity_id, trackpoints)

            self.db_connection.commit()

    def get_labeled_user_ids(self, data_path):
        labeled_ids = set()
        labeled_ids_file = os.path.join(data_path, 'labeled_ids.txt')
        if os.path.exists(labeled_ids_file):
            with open(labeled_ids_file, 'r') as f:
                for line in f:
                    user_id = line.strip()
                    if user_id:
                        labeled_ids.add(int(user_id))
        return labeled_ids

    def read_labels(self, user_path):
        labels = []
        labels_file_path = os.path.join(user_path, 'labels.txt')
        if os.path.exists(labels_file_path):
            with open(labels_file_path, 'r') as f:
                lines = f.readlines()
                for line in lines[1:]:  # Skip the header line
                    items = line.strip().split('\t')
                    if len(items) < 3:
                        continue  # Skip malformed lines
                    start_time_str = items[0]
                    end_time_str = items[1]
                    transportation_mode = items[2]
                    try:
                        start_time = datetime.strptime(start_time_str, '%Y/%m/%d %H:%M:%S')
                        end_time = datetime.strptime(end_time_str, '%Y/%m/%d %H:%M:%S')
                    except ValueError:
                        continue  # Skip lines with invalid date format
                    labels.append({
                        'transportation_mode': transportation_mode,
                        'start_time': start_time,
                        'end_time': end_time
                    })
        return labels

    def get_transportation_mode(self, labels, start_date_time, end_date_time):
        for label in labels:
            if label['start_time'] == start_date_time and label['end_time'] == end_date_time:
                return label['transportation_mode']
        return None  # No exact match found

    def read_trajectory_file(self, trajectory_file_path):
        trackpoints = []
        with open(trajectory_file_path, 'r') as f:
            lines = f.readlines()[6:]  # Skip the first 6 lines
            for line in lines:
                items = line.strip().split(',')
                if len(items) < 7:
                    continue  # Skip malformed lines
                lat = float(items[0])
                lon = float(items[1])
                # Field 3 is ignored
                altitude = float(items[3]) if items[3] != '-777' else None
                date_days = float(items[4])
                date_time_str = items[5] + ' ' + items[6]
                try:
                    date_time = datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    continue  # Skip lines with invalid date format
                trackpoints.append({
                    'lat': lat,
                    'lon': lon,
                    'altitude': altitude,
                    'date_days': date_days,
                    'date_time': date_time
                })
        return trackpoints

    def insert_trackpoints(self, activity_id, trackpoints):
        trackpoint_values = []
        for tp in trackpoints:
            trackpoint_values.append((
                activity_id,
                tp['lat'],
                tp['lon'],
                tp['altitude'],
                tp['date_days'],
                tp['date_time']
            ))
        # Batch insert
        insert_query = """
        INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        self.cursor.executemany(insert_query, trackpoint_values)

    def close(self):
        self.connection.close_connection()

def main():
    data_inserter = DataInserter()
    try:
        data_inserter.create_tables()
        data_path = '/home/folkej/ntnuhome/Documents/TDT4225/Oppg_2/dataset'
        data_inserter.insert_data(data_path)
    except Exception as e:
        print("An error occurred:", e)
    finally:
        data_inserter.close()

if __name__ == '__main__':
    main()
 
