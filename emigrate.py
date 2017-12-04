import fdb
import os
import sys
import time
import json
import subprocess

HISTORY_NAME = 'history.json'
# MIGRATIONS = os.getenv('EMIGRATE_MIGRATIONS', '/migrations')
# HOST = os.getenv('EMIGRATE_HOST', 'localhost')
# PORT = os.getenv('EMIGRATE_PORT', 3050)
# PATH = os.getenv('EMIGRATE_PATH', '/var/lib/firebird/2.5/data/')
# DATABASE = os.getenv('EMIGRATE_DATABASE', 'firebird.fdb')
# USER = os.getenv('EMIGRATE_USER', 'SYSDBA')
# PASSWORD = os.getenv('EMIGRATE_PASSWORD', 'masterkey')
# ISQL = os.getenv('EMIGRATE_ISQL', '/usr/bin/isql-fb)
MIGRATIONS = '/home/hunsche/projects/prospera/prospera-erp-api/migrations/common'
HOST = 'localhost'
PORT = 32773
PATH = '/var/lib/firebird/2.5/data/common/'
DATABASE = 'common.fdb'
USER = 'SYSDBA'
PASSWORD = 'masterkey'
ISQL = '/usr/bin/isql-fb'


def get_date_on_name_file(file):
    date_string = file[0:4]
    spacing = 4
    for i in range(5):
        date_string += ' ' + file[spacing:spacing + 2]
        spacing += 2
    date = time.strptime(date_string, "%Y %m %d %H %M %S")
    return date


def get_connection():
    return fdb.connect(
        host=HOST,
        port=PORT,
        database=os.path.join(PATH, DATABASE),
        user=USER,
        password=PASSWORD)


def get_migrations():
    ultimate_migrate = get_ultimate_migrate_executed()
    migrations = {}
    for file_name in os.listdir(MIGRATIONS):
        if file_name.endswith(".sql"):

            if ultimate_migrate:
                if get_date_on_name_file(file_name) <= get_date_on_name_file(
                        ultimate_migrate):
                    continue

            script = open(os.path.join(MIGRATIONS, file_name), 'r').read()
            commands = script
            migrations[get_date_on_name_file(file_name)] = {"file_name": file_name}
            # {
            #     "file_name": file_name, "commands": commands
            # }

    sorted(migrations)
    return migrations


def get_curdir_migrate():
    return MIGRATIONS.split("/")[-1]


def get_ultimate_migrate_executed():
    if not os.path.exists(HISTORY_NAME):
        file_json = open(HISTORY_NAME, 'w')
        data = {}
        data_json = json.dumps(data, sort_keys=True, indent=4)
        file_json.write(data_json)
        file_json.close

    file_json = open(HISTORY_NAME, 'r')
    data = json.load(file_json)
    file_json.close()

    if data:
        return data[get_curdir_migrate()]


def set_ultimate_migrate_executed(file_name):
    file_json = open(HISTORY_NAME, 'r')
    data = json.load(file_json)
    file_json.close()

    file_json = open(HISTORY_NAME, 'w')
    data[get_curdir_migrate()] = file_name
    data_json = json.dumps(data, sort_keys=True, indent=4)
    file_json.write(data_json)
    file_json.close

def execute_migrations():
    migrations = get_migrations()
    for script in migrations:
        print("-u " + USER + " -p " + PASSWORD + " " + os.path.join(PATH, DATABASE) + " -i " + os.path.join(MIGRATIONS, migrations[script]["file_name"]))
        subprocess.call([ISQL, "-u " + USER + " -p " + PASSWORD + " " + os.path.join(PATH, DATABASE) + " -i " + os.path.join(MIGRATIONS, migrations[script]["file_name"])])
        set_ultimate_migrate_executed(migrations[script]["file_name"])

def main(argv):
    execute_migrations()

if __name__ == "__main__":
    main(sys.argv)