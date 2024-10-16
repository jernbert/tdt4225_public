from DbConnector import DbConnector
import math
from tabulate import tabulate  # Import tabulate

class Task10:
    def __init__(self):
        self.connection = DbConnector()
        self.cursor = self.connection.cursor

    def haversine(self, lat1, lon1, lat2, lon2):
        # Earth radius in meters
        R = 6371000

        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        delta_phi = math.radians(lat2 - lat1)
        delta_lambda = math.radians(lon2 - lon1)

        a = math.sin(delta_phi / 2.0) ** 2 + \
            math.cos(phi1) * math.cos(phi2) * \
            math.sin(delta_lambda / 2.0) ** 2

        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        return R * c  # Output distance in meters

    def find_users_in_forbidden_city(self):
        forbidden_city_lat = 39.916
        forbidden_city_lon = 116.397
        radius = 100  # 100 meters

        # Retrieve all trackpoints near the Forbidden City
        # The +/- 0.001 widens the radius to be filtered exactly later in the code
        self.cursor.execute("""
            SELECT DISTINCT A.user_id
            FROM TrackPoint T
            JOIN Activity A ON T.activity_id = A.id
            WHERE T.lat BETWEEN %s AND %s AND T.lon BETWEEN %s AND %s;
        """, (forbidden_city_lat - 0.001, forbidden_city_lat + 0.001,
              forbidden_city_lon - 0.001, forbidden_city_lon + 0.001))
        possible_users = set([row[0] for row in self.cursor.fetchall()])

        users_in_area = set()

        for user_id in possible_users:
            # Get trackpoints for the user near the Forbidden City
            self.cursor.execute("""
                SELECT T.lat, T.lon
                FROM TrackPoint T
                JOIN Activity A ON T.activity_id = A.id
                WHERE A.user_id = %s AND
                      T.lat BETWEEN %s AND %s AND
                      T.lon BETWEEN %s AND %s;
            """, (user_id,
                  forbidden_city_lat - 0.001, forbidden_city_lat + 0.001,
                  forbidden_city_lon - 0.001, forbidden_city_lon + 0.001))
            trackpoints = self.cursor.fetchall()

            for lat, lon in trackpoints:
                distance = self.haversine(lat, lon, forbidden_city_lat, forbidden_city_lon)
                if distance <= radius:
                    users_in_area.add(user_id)
                    break  # No need to check further for this user

        # Format the output using tabulate
        table_data = [[user_id] for user_id in users_in_area]
        print(tabulate(table_data, headers=["User ID"], tablefmt="grid"))

    def close(self):
        self.connection.close_connection()

def main():
    task = Task10()
    try:
        task.find_users_in_forbidden_city()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        task.close()

if __name__ == "__main__":
    main()
