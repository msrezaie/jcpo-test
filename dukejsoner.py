import psycopg2
import json
from decouple import config


def connect_db():
    try:
        conn = psycopg2.connect(
            database=config("DB_NAME"),
            user=config("DB_USER"),
            password=config("DB_PASS"),
            host=config("DB_SERVICE"),
            port=config("DB_PORT"),
        )
    except Exception as e:
        print(f"Error connecting to the database: {str(e)}")
    finally:
        return conn


def fetch_outage_tracker():
    conn = connect_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM outage_tracker")
        rows = cur.fetchall()

        # Convert query to objects of key-value pairs
        objects_list = []
        for row in rows:
            d = {}
            d["outage_identifer"] = str(row[0])
            d["device_lat"] = float(row[1])
            d["device_lon"] = float(row[2])
            d["block_fips"] = float(row[3]) if row[3] is not None else None
            d["convex_hull"] = str(row[4])
            d["jurisdiction"] = str(row[5])
            d["origin"] = str(row[6])
            d["state"] = str(row[7])
            d["county"] = str(row[8])
            d["affected"] = str(row[9])
            d["cause"] = str(row[10])
            d["outage_start_estimate"] = str(row[11])
            d["outage_end_estimate"] = str(row[12])
            d["fix_duration_estimate"] = str(row[13])
            d["outage_restored"] = str(row[14])
            objects_list.append(d)

        cur.close()

        return json.dumps(objects_list, indent=4)

    except Exception as e:
        print(f"Error fetching data: {str(e)}")
    finally:
        conn.close()


def fetch_duke_outages():
    try:
        conn = connect_db()
        cur = conn.cursor()
        cur.execute("SELECT * FROM duke_outages")
        rows = cur.fetchall()

        # Convert query to objects of key-value pairs
        objects_list = []
        for row in rows:
            d = {}
            d["outage_identifer"] = str(row[0])
            d["device_lat"] = float(row[1])
            d["device_lon"] = float(row[2])
            d["convex_hull"] = str(row[3])
            d["jurisdiction"] = str(row[4])
            d["affected"] = int(row[5])
            d["cause"] = str(row[6])
            d["created_at"] = str(row[7])
            objects_list.append(d)

        cur.close()

        return json.dumps(objects_list, indent=4)

    except Exception as e:
        print(f"Error fetching data: {str(e)}")
    finally:
        conn.close()


def main():
    with open("outage_tracker.json", "w") as f:
        f.write(fetch_outage_tracker())

    with open("duke_outages.json", "w") as f:
        f.write(fetch_duke_outages())


if __name__ == "__main__":
    try:
        main()
        print("Outage data saved in 'outage_tracker.json' and 'duke_outages.json' files!.")
    except Exception as e:
        print(f"Task failed with error: {e}")
