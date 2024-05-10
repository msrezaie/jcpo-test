# DEC Outage Report

This repository contains the DEC Outage Report project.

## Installation Guide

To install and run the project locally, follow these steps:

1. Clone the repository:

   ```
   git clone https://github.com/your-username/JC-outage.git
   ```

2. Install the required dependencies:

   ```
   pip install -r requirements.txt
   ```

3. The Database used for saving the outage data is PostgreSQL. An instance of the DB needs to be installed in order to run the script. Follow this link for download and installation
   https://www.postgresql.org/download/

4. Create and add an .env file into the repo's root directory with the following keys and values specific to your configuration:
   ```
   DB_NAME = 
   DB_USER =
   DB_PASS =
   DB_SERVICE =
   DB_PORT =
   ```

## Scripts

- decscheduler.py: a script that runs a scheduled task (implemented using '[schedule](https://pypi.org/project/schedule/)' package) in 10 minute intervals and makes use of the `decoutages.py` script which fetches the outage data from the Duke Power API for DEC region and saves the data into two PostgreSQL tables called `dec_outages` and `outage_tracker`.

```
python "path to file"\decscheduler.py
```

## Viewing the data
There are two ways to viewing the fetched outage data:
- decjsoner.py: simply run this script and the two tables data is going to be saved and stored into two separate JSON files.
```
python "path to file"\decjsoner.py
```

- pgAdmin: the saved data in the db can be viewed in the PostgreSQL admin platform called pgAdmin. Download and setup using this [link](https://www.pgadmin.org/download/).
