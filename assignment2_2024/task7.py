# task7.py

import math
from DbConnector import DbConnector
from datetime import datetime

class Task7:
    def __init__(self):
        self.connection = DbConnector()
        self.cursor = self.connection.cursor

    def haversine(self, lat1, lon1, lat2, lon2):
        # Earth radius in kilometers
        R = 6371.0

        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        delta_phi = math.radians(lat2 - lat1)
        delta_lambda = math.radians(lon2 - lon1)

        a = math.sin(delta_phi / 2.0) ** 2 + \
            math.cos(phi1) * math.cos(phi2) * \
            math.sin(delta_lambda / 2.0) ** 2

        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        return R * c  # Output distance in kilometers

    def calculate_total_distance(self):
        # Retrieve activities for user 112, transportation_mode 'walk', year 2008
        query = """
        SELECT id
        FROM Activity
        WHERE user_id = 112
          AND transportation_mode = 'walk'
          AND YEAR(start_date_time) = 2008;
        """
        self.cursor.execute(query)
        activities = [row[0] for row in self.cursor.fetchall()]

        total_distance = 0.0

        for activity_id in activities:
            # Retrieve ordered trackpoints for the activity
            self.cursor.execute("""
                SELECT lat, lon
                FROM TrackPoint
                WHERE activity_id = %s
                ORDER BY date_time ASC;
            """, (activity_id,))
            trackpoints = self.cursor.fetchall()

            # Calculate distance between consecutive trackpoints
            for i in range(1, len(trackpoints)):
                lat1, lon1 = trackpoints[i - 1]
                lat2, lon2 = trackpoints[i]
                distance = self.haversine(lat1, lon1, lat2, lon2)
                total_distance += distance

        print(f"Total distance walked by user 112 in 2008: {total_distance:.2f} km")

    def close(self):
        self.connection.close_connection()

def main():
    task = Task7()
    try:
        task.calculate_total_distance()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        task.close()

if __name__ == "__main__":
    main()
