import fdb
import os
import sys
import time
import json
import subprocess

HISTORY_NAME = 'history.json'
ISQL = os.getenv('EMIGRATE_ISQL', '/usr/bin/isql-fb')
BASH = os.getenv('EMIGRATE_BASH', 'bash')
MIGRATIONS = os.getenv('EMIGRATE_MIGRATIONS', '/migrations')
HOST = os.getenv('EMIGRATE_HOST', 'localhost')
PORT = os.getenv('EMIGRATE_PORT', 3050)
PATH = os.getenv('EMIGRATE_PATH', '/var/lib/firebird/2.5/data/')
DATABASE = os.getenv('EMIGRATE_DATABASE', 'firebird.fdb')
USER = os.getenv('EMIGRATE_USER', 'SYSDBA')
PASSWORD = os.getenv('EMIGRATE_PASSWORD', 'masterkey')


def get_date_on_name_file(file):
    date_string = file[0:4]
    spacing = 4
    for i in range(5):
        date_string += ' ' + file[spacing:spacing + 2]
        spacing += 2
    date = time.strptime(date_string, "%Y %m %d %H %M %S")
    return date


def check_database():
    try:
        connect = fdb.connect(
            host=HOST,
            port=PORT,
            database=os.path.join(PATH, DATABASE),
            user=USER,
            password=PASSWORD)
        print('Connected in %s.' % (DATABASE))
    except:
        connect = fdb.create_database(
            host=HOST,
            port=PORT,
            database=os.path.join(PATH, DATABASE),
            user=USER,
            password=PASSWORD)
        print('Create database %s.' % (DATABASE))

    return connect


def get_migrations():
    ultimate_migrate = get_ultimate_migrate_executed()
    migrations = []
    for file_name in os.listdir(MIGRATIONS):
        if file_name.endswith(".sql"):

            if ultimate_migrate:
                if get_date_on_name_file(file_name) <= get_date_on_name_file(
                        ultimate_migrate):
                    continue

            script = open(os.path.join(MIGRATIONS, file_name), 'r').read()
            commands = script
            migrations.append(file_name)

    migrations.sort()
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
    if not migrations:
        print('No migrations to run.')
    for file_script in migrations:
        database = "%s/%d:%s" % (HOST, PORT, os.path.join(PATH, DATABASE))
        sql = os.path.join(MIGRATIONS, file_script)
        execute = "%s -u %s -p %s %s -i %s" % (ISQL, USER, PASSWORD, database, sql)
        print('Running %s .......................' % (file_script), end="")
        subprocess.call([BASH, '-c', execute])
        set_ultimate_migrate_executed(file_script)
        print('ok')


def main(argv):
    check_database()
    execute_migrations()


if __name__ == "__main__":
    main(sys.argv)