#!/usr/bin/env python

import os
import argparse
import sqlite3
from collections import defaultdict
import hashlib
 
def create_db(db_file_name):

    db_exists = os.path.exists(db_file_name)

    db_conn = sqlite3.connect(db_file_name)

    if db_exists:
        print('Clearing existing database...')
        db_conn.execute("drop table files")
    else:
        print('Creating database...')

    schema = """
        create table files (
            dir_name    text        not null,
            file_name   text        not null,
            file_size   integer     not null,
            file_hash   text
        );
    """
    db_conn.execute(schema)

    return db_conn

def insert(db_conn, dir_name, file_name, file_size):
    sql = """
        insert into files (dir_name, file_name, file_size, file_hash)
        values (:dir_name, :file_name, :file_size, NULL)
    """
    db_conn.execute(sql, {'dir_name':dir_name, 'file_name':file_name, 'file_size':file_size})

def get_files_with_same_hash(db_conn):
    sql = """
        select dir_name, file_name, file_hash
        from files
        where file_hash in (
            select file_hash
            from files
            group by file_hash
            having count(*) > 1
        )
        order by file_hash
    """

    cursor = db_conn.cursor()

    cursor.execute(sql)

    files_with_same_hash = defaultdict(list)
    for row in cursor.fetchall():
        dir_name, file_name, file_hash = row
        files_with_same_hash[file_hash].append((dir_name, file_name))

    return files_with_same_hash


def get_files_with_same_size(db_conn):
    sql = """
        select dir_name, file_name, file_size
        from files
        where file_size in (
            select file_size
            from files
            group by file_size
            having count(*) > 1
        )
        order by file_size
    """

    cursor = db_conn.cursor()

    cursor.execute(sql)

    files_with_same_size = defaultdict(list)
    for row in cursor.fetchall():
        dir_name, file_name, file_size = row
        files_with_same_size[file_size].append((dir_name, file_name))

    return files_with_same_size

def get_file_hash(dir_name, file_name):

    # Read file in 64 KB chunks in case it's really big
    BUF_SIZE = 65536
    sha1 = hashlib.sha1()

    with open(os.path.join(dir_name, file_name), 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            sha1.update(data)

    return sha1.hexdigest();

def update_hash(db_conn, dir_name, file_name, file_hash):
    sql = """
        update files
        set file_hash = :file_hash
        where dir_name = :dir_name
        and file_name = :file_name
    """
    db_conn.execute(sql, {'file_hash':file_hash, 'dir_name':dir_name, 'file_name':file_name})


def main():

    parser = argparse.ArgumentParser(description='Identify duplicate files')
    parser.add_argument('Starting path', metavar='path', help='Path to start the directory walk from')

    args = vars(parser.parse_args())
    root_dir = args.get("Starting path")

    db_file = "files.db"
    db_conn = create_db(db_file)

    # Walk the directory tree and insert a row into the DB for each file found.
    # At this point we're using the file size as a very quick hash
    print("Walking directories...")
    for dir_name, subdirList, fileList in os.walk(root_dir):
        for file_name in fileList:
            file_size = os.path.getsize(os.path.join(dir_name, file_name))
            insert(db_conn, dir_name, file_name, file_size)

    # Now look at all the files with the same size and calculate a proper hash
    print("Calculating hashes...")
    files_with_same_size = get_files_with_same_size(db_conn)
    for file_size in files_with_same_size:
        for (dir_name, file_name) in files_with_same_size[file_size]:
            file_hash = get_file_hash(dir_name, file_name)
            update_hash(db_conn, dir_name, file_name, file_hash)

    print("Files with identical hashes...")
    files_with_same_hash = get_files_with_same_hash(db_conn)
    for file_hash in files_with_same_hash:
        for (dir_name, file_name) in files_with_same_hash[file_hash]:
            print("hash: %s => %s " % (file_hash, os.path.join(dir_name, file_name)))

    db_conn.close()
    os.remove(db_file)

main()
