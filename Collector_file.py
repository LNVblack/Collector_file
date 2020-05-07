import psycopg2
import os
import hashlib
import configparser
import time
import sys

def get_hash_md5(filename):
    with open(filename, "rb") as f:
        m = hashlib.md5()
        while True:
            data = f.read(8192)
            if not data:
                break
            m.update(data)
        return str(m.hexdigest())

def get_GUID(cursor, file_path):
    cursor.callproc("datahouse.reg_file_md5", [get_hash_md5(file_path)]) 
    for row in cursor:
        return row[0]

def get_list_paths():
    first_path = r"".join(sys.argv[1])
    tree = os.walk(r"" + first_path)
    folder = []
    for i in tree:
        folder.append(i)
    return folder

def open_ini():
    Config = configparser.ConfigParser()
    Config.read(r"config.ini")
    Config.sections()
    return Config

def get_hostname_of_config():
    Config = open_ini()
    return Config.get("HostName", "hostname")

def get_conn_list_of_config():
    Config = open_ini()
    return [Config.get("DB Connection", "dbname"), Config.get("DB Connection", "user"), Config.get("DB Connection", "password"), Config.get("DB Connection", "host")]

def get_file_date_time(filepath):
    time_stamp = os.path.getmtime(file_path)
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time_stamp))

if __name__ == "__main__":
    print("start")
    conn_list = get_conn_list_of_config()
    conn = psycopg2.connect(dbname="%s"%(conn_list[0]), user="%s"%(conn_list[1]), password="%s"%(conn_list[2]), host="%s"%(conn_list[3]))
    cursor = conn.cursor()
    folder = get_list_paths()
    for address, dirs, files in folder: 
        for file in files:
            file_path = address + "/" + file
            if "/~$" not in file_path:
                cursor.callproc("datahouse._add_fileversion", [get_GUID(cursor, file_path), file, os.path.getsize(file_path), get_file_date_time(file_path), get_hostname_of_config(), address])
                print(file, os.path.getsize(file_path), get_file_date_time(file_path), get_hostname_of_config(), address)
                conn.commit()
    cursor.close()
    conn.close()
    print("stop")