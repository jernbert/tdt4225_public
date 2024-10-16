# task8.py

from DbConnector import DbConnector
from tabulate import tabulate

class Task8:
    def __init__(self):
        self.connection = DbConnector()
        self.cursor = self.connection.cursor

    def calculate_altitude_gain(self):
        # Get all user IDs
        self.cursor.execute("SELECT id FROM User;")
        users = [row[0] for row in self.cursor.fetchall()]

        user_gains = []

        for user_id in users:
            # Retrieve trackpoints for the user, ordered by activity and time
            self.cursor.execute("""
                SELECT T.altitude
                FROM TrackPoint T
                JOIN Activity A ON T.activity_id = A.id
                WHERE A.user_id = %s AND T.altitude IS NOT NULL
                ORDER BY A.id, T.date_time ASC;
            """, (user_id,))
            altitudes = [row[0] for row in self.cursor.fetchall()]

            total_gain = 0.0
            for i in range(1, len(altitudes)):
                if altitudes[i] > altitudes[i - 1]:
                    total_gain += altitudes[i] - altitudes[i - 1]

            user_gains.append((user_id, total_gain))

        # Sort users by total_gain in descending order
        user_gains.sort(key=lambda x: x[1], reverse=True)

        # Prepare the data for tabulate
        table_data = []
        for user_id, gain in user_gains[:20]:
            table_data.append([user_id, f"{gain:,.2f}"])

        # Print the top 20 users by altitude gain in a tabulated format
        print(tabulate(table_data, headers=["User ID", "Total Gain (m)"], tablefmt="grid"))

    def close(self):
        self.connection.close_connection()

def main():
    task = Task8()
    try:
        task.calculate_altitude_gain()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        task.close()

if __name__ == "__main__":
    main()
