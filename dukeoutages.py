#!/usr/bin/env python3
# coding: utf-8

import requests
import json
from decouple import config
import psycopg2
from datetime import datetime

# For area data, census block, county, state, and market area information based on latitude/longitude input
area_url = "https://geo.fcc.gov/api/census/area"
geo_url = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"

# For Duke outages location data
# counties_url = (
#     "https://prod.apigee.duke-energy.app/outage-maps/v1/counties?jurisdiction="
# )

# For outages data with latitudes and longitudes
outages_url = "https://prod.apigee.duke-energy.app/outage-maps/v1/outages?jurisdiction="

# Constants
censusYear = 2020
jurisdictions = [
    "DEF",   # Duke Energy Florida
    "DEC",   # Duke Energy Carolinas
    "DEI",   # Duke Energy Indiana
    "DEM"    # Duke Energy Ohio and Kentucky, which somehow have an M in them.
]


"""
Method for hitting the FCC API to get area data based on latitude and longitude
Note: Either this method or the hit_geo method can be used, not both
"""


def hit_fcc(nugget):
    lat = nugget["device_lat"]
    lon = nugget["device_lon"]

    area_request = requests.get(
        f"{area_url}?lat={lat}&lon={lon}&censusYear={censusYear}&format=json"
    )

    area_data = json.loads(area_request.content)
    for result in area_data["results"]:
        state = result["state_name"]
        county = result["county_name"]
        block_fips = result["block_fips"]

        entry = {
            "block_fips": block_fips,
            "state": state,
            "county": county,
        }

    return entry


"""
Method for hitting the Census geographies API to get area data based on latitude and longitude
Note: Either this method or the hit_fcc method can be used, not both
"""


def hit_geo(nugget):
    lat = nugget["device_lat"]
    lon = nugget["device_lon"]
    geo_request = requests.get(
        f"{geo_url}?x={lon}&y={lat}&benchmark=4&vintage=423&format=json"
    )
    geo_data = json.loads(geo_request.content)

    entry = {}
    geographies = geo_data["result"]["geographies"]

    if 'States' in geographies:
        entry['state'] = geographies['States'][0]['NAME']

    if 'Counties' in geographies:
        county_data = geographies['Counties'][0]
        entry['county'] = county_data['NAME']
        entry['fips'] = county_data['GEOID']

    if 'Census Tracts' in geographies:
        entry['tract'] = geographies['Census Tracts'][0]['TRACT']

    return entry


"""
Method for hitting the Duke Power API to get outage data based on jurisdiction
"""


def hit_duke(jurisdictions, headers, cookies):
    master = []

    for jurisdiction in jurisdictions:
        outages_res = requests.get(
            f"{outages_url}{jurisdiction}", headers=headers, cookies=cookies
        )

        outages_data = json.loads(outages_res.content)

        for nugget in outages_data["data"]:
            source_event_number = nugget["sourceEventNumber"]
            device_lat = nugget["deviceLatitudeLocation"]
            device_lon = nugget["deviceLongitudeLocation"]
            jurisdiction = jurisdiction
            affected = nugget["customersAffectedNumber"]
            cause = nugget["outageCause"]
            convex_hull = nugget["convexHull"]

            entry = {
                "source_event_number": source_event_number,
                "device_lat": device_lat,
                "device_lon": device_lon,
                "convex_hull": convex_hull,
                "jurisdiction": jurisdiction,
                "affected": affected,
                "cause": cause,
                "origin": "Duke Energy",
            }

            master.append(entry)

    return master


"""
Method for creating duke_outages, and
outage_tracker tables in the database
"""


def create_tables():
    conn = connect_db()
    try:
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS duke_outages (
                outage_identifer TEXT UNIQUE,
                device_lat DECIMAL,
                device_lon DECIMAL,
                convex_hull JSONB,
                jurisdiction TEXT,
                affected INTEGER,
                cause TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS outage_tracker (
                outage_identifer TEXT UNIQUE,
                device_lat DECIMAL,
                device_lon DECIMAL,
                block_fips DECIMAL,
                convex_hull JSONB,
                jurisdiction TEXT,
                origin TEXT, 
                state TEXT,
                county TEXT,
                affected INTEGER,
                cause TEXT,
                outage_start_estimate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                outage_end_estimate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                fix_duration_estimate TEXT,
                outage_restored BOOLEAN
            )
        """)

        conn.commit()
    except Exception as e:
        print(f"Error creating tables: {str(e)}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


"""
Method for saving outage data to the duke_outages table
"""


def save_outages(data):
    try:
        conn = connect_db()
        try:
            cur = conn.cursor()

            cur.execute("""
                CREATE TABLE IF NOT EXISTS duke_outages (
                    outage_identifer TEXT,
                    device_lat DECIMAL,
                    device_lon DECIMAL,
                    convex_hull JSONB,
                    jurisdiction TEXT,
                    affected INTEGER,
                    cause TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            for entry in data:
                try:
                    entry['convex_hull'] = json.dumps(entry['convex_hull'])

                    cur.execute("""
                        SELECT COUNT(*) FROM duke_outages
                        WHERE outage_identifer = %(source_event_number)s
                    """, entry)
                    count = cur.fetchone()[0]

                    if count == 0:
                        cur.execute("""
                            INSERT INTO duke_outages (
                                outage_identifer,
                                device_lat,
                                device_lon,
                                convex_hull,
                                jurisdiction,
                                affected,
                                cause
                            )
                            VALUES (
                                %(source_event_number)s,
                                %(device_lat)s,
                                %(device_lon)s,
                                %(convex_hull)s,
                                %(jurisdiction)s,
                                %(affected)s,
                                %(cause)s
                            )
                        """, entry)
                except Exception as e:
                    print(f"Error inserting entry: {entry}")
                    print(f"Error message: {str(e)}")
                    conn.rollback()

            conn.commit()
        except Exception as e:
            print(f"Error executing database operations: {str(e)}")
            conn.rollback()
        finally:
            cur.close()
    except Exception as e:
        print(f"Error connecting to the database: {str(e)}")
    finally:
        conn.close()


"""
Method for saving outage data to the outage_tracker table
"""


def save_tracker(data):
    try:
        conn = connect_db()
        try:
            cur = conn.cursor()

            cur.execute("""
                CREATE TABLE IF NOT EXISTS outage_tracker (
                    outage_identifer TEXT,
                    device_lat DECIMAL,
                    device_lon DECIMAL,
                    block_fips DECIMAL,
                    convex_hull JSONB,
                    jurisdiction TEXT,
                    origin TEXT,
                    state TEXT,
                    county TEXT,
                    affected INTEGER,
                    cause TEXT,
                    outage_start_estimate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    outage_end_estimate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    fix_duration_estimate TEXT,
                    outage_restored BOOLEAN
                )
            """)

            for entry in data:
                try:

                    # Check if the entry already exists in the tracker table based on source_event_number
                    cur.execute("""
                        SELECT COUNT(*) FROM outage_tracker
                        WHERE outage_identifer = %(source_event_number)s
                    """, entry)
                    count = cur.fetchone()[0]

                    if count == 0:
                        additional_data = hit_fcc(entry) or {}

                        # Merge the additional data into the entry dictionary
                        entry['block_fips'] = additional_data['block_fips'] if 'block_fips' in additional_data else None
                        entry['state'] = additional_data['state']
                        entry['county'] = additional_data['county']

                        # entry['fips'] = additional_data['fips']
                        # entry['tract'] = additional_data['tract']

                        entry['outage_restored'] = False

                        cur.execute("""
                            INSERT INTO outage_tracker (
                                outage_identifer,
                                device_lat,
                                device_lon,
                                block_fips,
                                convex_hull,
                                jurisdiction,
                                origin,
                                state,
                                county,
                                affected,
                                cause,
                                outage_restored
                            )
                            VALUES (
                                %(source_event_number)s,
                                %(device_lat)s,
                                %(device_lon)s,
                                %(block_fips)s,
                                %(convex_hull)s,
                                %(jurisdiction)s,
                                %(origin)s,
                                %(state)s,
                                %(county)s,
                                %(affected)s,
                                %(cause)s,
                                %(outage_restored)s
                            )
                        """, entry)
                except Exception as e:
                    print(f"Error inserting entry: {entry}")
                    print(f"Error message: {str(e)}")
                    conn.rollback()

            conn.commit()
        except Exception as e:
            print(f"Error executing database operations: {str(e)}")
            conn.rollback()
        finally:
            cur.close()

    except Exception as e:
        print(f"Error connecting to the database: {str(e)}")


"""
Method for updating the outage_tracker table
"""


def update_tracker(data):
    try:
        current_datetime = datetime.now()
        formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")

        conn = connect_db()
        try:
            cur = conn.cursor()

            cur.execute("""
                SELECT outage_identifer FROM outage_tracker
            """)
            tracker_identifers = [row[0] for row in cur.fetchall()]

            """
            Mark entries as outage_restored if they are no longer
            in the data from Duke Power API, and update the 'ended_estimate'
            with the current time and 'fix_duration_estimate' field
            based on the difference between the 'start_estimate' field
            and current time
            """
            for event_number in tracker_identifers:
                try:
                    if event_number not in [entry['source_event_number'] for entry in data]:

                        cur.execute("""
                            SELECT outage_start_estimate
                            FROM outage_tracker
                            WHERE outage_identifer = %(outage_identifer)s
                        """, {'outage_identifer': event_number})
                        start_estimate = cur.fetchone()[0]

                        formatted_datetime = datetime.now()

                        duration = formatted_datetime - start_estimate
                        """
                        better to be number of seconds to be calculated from ended - started
                        """
                        # Convert the duration to days, hours, minutes, and seconds
                        days = duration.days
                        hours = duration.seconds // 3600
                        minutes = (duration.seconds // 60) % 60
                        seconds = duration.seconds % 60

                        fix_duration_estimate = f"{days}d {hours}h {minutes}m {seconds}s"

                        cur.execute("""
                            UPDATE outage_tracker
                            SET
                                outage_restored = true,
                                outage_end_estimate = CASE
                                    WHEN outage_restored = false THEN %(outage_end_estimate)s
                                    ELSE outage_end_estimate
                                END,
                                fix_duration_estimate = CASE
                                    WHEN outage_restored = false THEN %(fix_duration_estimate)s
                                    ELSE fix_duration_estimate
                                END
                            WHERE outage_identifer = %(outage_identifer)s
                        """, {
                            'outage_end_estimate': formatted_datetime,
                            'outage_identifer': event_number,
                            'fix_duration_estimate': fix_duration_estimate
                        })
                except Exception as e:
                    print(
                        f"Error in update_tracker method - Marking outage as restored: {event_number}")
                    print(f"Error message: {str(e)}")
                    conn.rollback()

            conn.commit()
        except Exception as e:
            print(
                f"Error in update_tracker method - Executing database operations: {str(e)}")
            conn.rollback()
        finally:
            cur.close()
    except Exception as e:
        print(
            f"Error in update_tracker method - Connecting to the database: {str(e)}")
    finally:
        conn.close()


"""
Method for connecting to the database
"""


def connect_db():
    try:
        conn = psycopg2.connect(config("REMOTE_DB_SERVICE"))
    except Exception as e:
        print(f"Error connecting to the database: {str(e)}")
    finally:
        return conn


"""
Main method for running all the methods
"""


def main(headers, cookies):
    # Create tables if they don't exist
    create_tables()

    # Fetch outage data from Duke Power API
    data = hit_duke(jurisdictions, headers, cookies)

    # Save outage data to the tables
    save_outages(data)
    save_tracker(data)

    # # Update tracker table
    update_tracker(data)
