from DbConnector import DbConnector
from datetime import timedelta
from tabulate import tabulate 

class Task9:
    def __init__(self):
        self.connection = DbConnector()
        self.cursor = self.connection.cursor

    def find_invalid_activities(self):
        # Retrieve all activities
        self.cursor.execute("""
            SELECT id, user_id
            FROM Activity;
        """)
        activities = self.cursor.fetchall()

        user_invalid_counts = {}

        for activity_id, user_id in activities:
            # Retrieve trackpoints for the activity
            self.cursor.execute("""
                SELECT date_time
                FROM TrackPoint
                WHERE activity_id = %s
                ORDER BY date_time ASC;
            """, (activity_id,))
            date_times = [row[0] for row in self.cursor.fetchall()]

            invalid = False
            for i in range(1, len(date_times)):
                time_diff = date_times[i] - date_times[i - 1]
                if time_diff >= timedelta(minutes=5):
                    invalid = True
                    break  # No need to check further for this activity

            if invalid:
                if user_id in user_invalid_counts:
                    user_invalid_counts[user_id] += 1
                else:
                    user_invalid_counts[user_id] = 1

        # Prepare the data for tabulate
        table_data = [[user_id, count] for user_id, count in user_invalid_counts.items()]

        # Output the users with invalid activities in a tabulated format
        print(tabulate(table_data, headers=["User ID", "Invalid Activity Count"], tablefmt="grid"))

    def close(self):
        self.connection.close_connection()

def main():
    task = Task9()
    try:
        task.find_invalid_activities()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        task.close()

if __name__ == "__main__":
    main()

