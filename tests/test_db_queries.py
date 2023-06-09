import tempfile
import shutil
import pandas
import numpy
import pytest
import os
import sys
from pathlib import Path
from contextlib import contextmanager

import plpipes.init
import plpipes.database as db

df = pandas.DataFrame({'a':[1, 4, 2], 'b':[2, 5, 9], 'c':[3, 6, 7]})

# Handling DB creation and destruction via a teporary file
@contextmanager
def handle_db():
    try:
        # TESTS SETUP
        # work_dir = extract_path(str(tempfile.TemporaryDirectory()))
        work_dir = Path(tempfile.TemporaryDirectory().name)
        plpipes.init.init({'fs.root': str(work_dir)}, {'logging.log_to_file': False})
        yield work_dir
    finally:
        # TESTS TEARDOWN
        db.engine().dispose()       # Close DB connection
        shutil.rmtree(work_dir)     # Delete temporary file

@pytest.fixture(scope="session")
def db_context():
    # Create a temporary directory
    with handle_db() as work_dir:
        yield work_dir

# CREATE TABLE test
# Checks if a table is properly created
def test_create_query(db_context):
    db.create_table("test_table", df)
    query_result = db.query("select * from test_table")
    assert df.equals(query_result)

# READ TABLE test
# Checks if a table is properly read
def test_read(db_context):
    db.create_table("test_table", df)
    reading_result = db.read_table("test_table")
    assert df.equals(reading_result)

# CREATE TABLE WITH APPEND (equivalent to INSERT query) test
# Checks if a data insertion is properly performed
def test_create_table_append(db_context):
    df1 = pandas.DataFrame({'a': [6], 'b': [7], 'c': [9]})
    df_expected = pandas.DataFrame({'a':[1, 4, 2, 6], 'b':[2, 5, 9, 7], 'c':[3, 6, 7, 9]})
    db.create_table("test_table", df)
    db.create_table("test_table", df1, if_exists="append")
    query_result = db.query("select * from test_table")
    assert df_expected.equals(query_result)
    
# COPY TABLE test
# Checks if a table is properly copied into other table
def test_copy_table_df(db_context):
    db.create_table("test_table", df)
    source_table_name = "test_table"
    dest_table_name = "test_table_copy"
    db.copy_table("test_table", "test_table_copy")
    query_result_orig_table = db.query("select * from test_table")
    query_result_copy_table = db.query("select * from test_table_copy")
    assert query_result_orig_table.equals(query_result_copy_table) 

# CREATE VIEW test
# Checks if a view from a query is properly created
def test_create_view(db_context):
    db.create_table("test_table", df)
    view = db.create_view("test_view", "select * from test_table")
    query_result_table = db.query("select * from test_table")
    query_result_view = db.query("select * from test_view")
    assert query_result_table.equals(query_result_view)

# EXECUTE tests
    # DELETE ROW test
    # Checks if a row from a table is properly deleted
def test_execute_delete(db_context):
    df_expected = pandas.DataFrame({'a':[4, 2], 'b':[5, 9], 'c':[6, 7]})
    db.create_table("test_table", df)
    db.execute("delete from test_table where a=1")
    query_result = db.query("select * from test_table")
    assert df_expected.equals(query_result)

    # ALTER TABLE ADD test
    # Checks if a column is properly added, with several datatypes
def test_execute_alter_add(db_context):
    df_expected = pandas.DataFrame({'a':[1, 4, 2], 'b':[2, 5, 9], 'c':[3, 6, 7], 'd':["first_value", 2, False]})
    db.create_table("test_table", df)
    query = db.query("select * from test_table")
    print(query.columns)
    db.execute("alter table test_table add column d")
    db.execute("update test_table set d = 'first_value' where a = 1")
    db.execute("update test_table set d = 2 where a = 4")
    db.execute("update test_table set d = False where a = 2")
    query_result = db.query("select * from test_table")
    assert df_expected.equals(query_result)

    # ALTER TABLE DROP test
    # Checks if a column is properly deleted
def test_execute_alter_drop(db_context):
    df_expected = pandas.DataFrame({'a':[1, 4, 2], 'b':[2, 5, 9]})
    db.create_table("test_table", df)
    db.execute("alter table test_table drop c")
    query_result = db.query("select * from test_table")
    assert df_expected.equals(query_result)

    # ALTER TABLE RENAME test
    # Checks if a column is properly renamed
def test_execute_alter_rename(db_context):
    df_expected = pandas.DataFrame({'a':[1, 4, 2], 'e':[2, 5, 9], 'c':[3, 6, 7]})
    db.create_table("test_table", df)
    db.execute("alter table test_table rename column b to e")
    query_result = db.query("select * from test_table")
    assert df_expected.equals(query_result)

    # DROP TABLE test
    # Checks if a table is properly deleted
def test_execute_drop(db_context):
    db.create_table("test_table", df)
    db.create_table("test_table_to_drop", df)
    query_result_before_dropping = db.query("select name from sqlite_schema where type='table'")
    db.execute("drop table test_table_to_drop")
    query_result_after_dropping = db.query("select name from sqlite_schema where type='table'")
    equal_dataframes = query_result_before_dropping.equals(query_result_after_dropping)
    assert equal_dataframes == False
   
# UPDATE TABLE tests
# Check if tables are updated properly, according to the key direction (strictly ascending, 
# ascending, descending or strictly descending)
    # UPDATE TABLE STRICTLY ASCENDING test
def test_update_table_strictly_ascending(db_context):
    df_dest = pandas.DataFrame({'a': [1, 3], 'b':[2, 5], 'c':[3, 5]})
    df_source = pandas.DataFrame({'a': [1, 3, 5], 'b': [2, 5, 7], 'c': [3, 5, 8]})
    df_expected = df_source
    source_table_name = "test_table_1"
    dest_table_name = "test_table_2"
    db.create_table(source_table_name, df_source)
    db.create_table(dest_table_name, df_dest)
    db.update_table(source_table_name, dest_table_name, key='a', key_dir=">")
    query_result = db.query("select * from test_table_2")
    assert df_expected.equals(query_result)

    # UPDATE TABLE ASCENDING test
def test_update_table_ascending(db_context):
    df_dest = pandas.DataFrame({'a': [1, 2], 'b':[2, 5], 'c':[3, 3]})
    df_source = pandas.DataFrame({'a': [1, 2, 2, 4], 'b': [2, 5, 6, 6], 'c': [3, 3, 5, 7]})
    df_expected = df_source
    source_table_name = "test_table_1"
    dest_table_name = "test_table_2"
    db.create_table(source_table_name, df_source)
    db.create_table(dest_table_name, df_dest)
    db.update_table(source_table_name, dest_table_name, key="a", key_dir=">=")
    query_result = db.query("select * from test_table_2")
    assert df_expected.equals(query_result)

    # UPDATE TABLE DESCENDING test
def test_update_table_descending(db_context):
    df_dest = pandas.DataFrame({'a': [5, 4], 'b':[7, 6], 'c':[7, 5]})
    df_source = pandas.DataFrame({'a': [5, 4, 4, 3], 'b': [7, 6, 6, 2], 'c': [7, 5, 3, 3]})
    df_expected = df_source
    source_table_name = "test_table_1"
    dest_table_name = "test_table_2"
    db.create_table(source_table_name, df_source)
    db.create_table(dest_table_name, df_dest)
    db.update_table(source_table_name, dest_table_name, key="a", key_dir="<=")
    query_result = db.query("select * from test_table_2")
    assert df_expected.equals(query_result)

    # UPDATE TABLE STRICTLY DESCENDING test
def test_update_table_strictly_descending(db_context):
    df_dest = pandas.DataFrame({'a': [3, 2], 'b':[4, 3], 'c':[5, 4]})
    df_source = pandas.DataFrame({'a': [3, 2, 1, 0], 'b': [4, 3, 2, 1], 'c': [5, 4, 3, 2]})
    df_expected = df_source
    source_table_name = "test_table_1"
    dest_table_name = "test_table_2"
    db.create_table(source_table_name, df_source)
    db.create_table(dest_table_name, df_dest)
    db.update_table(source_table_name, dest_table_name, key="a", key_dir="<")
    query_result = db.query("select * from test_table_2")
    assert df_expected.equals(query_result)    

# QUERY CHUNKED test
# Checks if the query is properly splitted up into chunks of size "chunksize"
    # Single chunk
def test_query_chunked_single_chunk(db_context):
    df_to_chunk = pandas.DataFrame({'a':[1, 4, 2], 'b':[2, 5, 9], 'c':[3, 6, 7], 'd':[3, 6, 7]})
    df_expected = df_to_chunk
    db.create_table("test_table", df_to_chunk)
    query_result = db.query_chunked("select * from test_table")
    query_result_from_generator = next(query_result)
    assert df_expected.equals(query_result_from_generator)

    # 2 chunks
def test_query_chunked_2_as_chunksize(db_context):
    df_to_chunk = pandas.DataFrame({'a':[1, 4, 2], 'b':[2, 5, 9], 'c':[3, 6, 7], 'd':[3, 6, 7]})
    df_expected = pandas.DataFrame({'a':[1, 4], 'b':[2, 5], 'c':[3, 6], 'd':[3, 6]})
    db.create_table("test_table", df_to_chunk)
    query_result = db.query_chunked("select * from test_table", chunksize=2)
    query_result_from_generator = next(query_result)
    assert df_expected.equals(query_result_from_generator)

    # 3 chunks
def test_query_chunked_3_as_chunksize(db_context):
    df_to_chunk = pandas.DataFrame({'a':[1, 4, 2, 7, 8, 0], 'b':[2, 5, 9, 4, 2, 3], 'c':[3, 6, 7, 5, 7, 7], 'd':[3, 8, 8, 3, 6, 7]})
    df_expected = pandas.DataFrame({'a':[1, 4, 2], 'b':[2, 5, 9], 'c':[3, 6, 7], 'd':[3, 8, 8]})
    db.create_table("test_table", df_to_chunk)
    query_result = db.query_chunked("select * from test_table", chunksize=3)
    query_result_from_generator = next(query_result)
    assert df_expected.equals(query_result_from_generator)

# QUERY GROUP test
# Checks if the query is properly grouped according to the values in the column set 
# in the "by" parameter
def test_query_group(db_context):
    df_to_chunk = pandas.DataFrame({'a':[1, 4, 2], 'b':[2, 5, 7], 'c':[3, 2, 3], 'd':[3, 6, 7]})
    df_expected_1 =  pandas.DataFrame({'index':[0], 'a':[4], 'b':[5], 'c':[2], 'd':[6]})
    df_expected_2 =  pandas.DataFrame({'index':[1, 2], 'a':[1, 2], 'b':[2, 7], 'c':[3, 3], 'd':[3, 7]})
    db.create_table("test_table", df_to_chunk)
    query_result = db.query_group("select * from test_table", by=["c"])
    query_result_from_generator_first_element = next(query_result)
    query_result_from_generator_second_element = next(query_result)
    assert (df_expected_1.equals(query_result_from_generator_first_element) and df_expected_2.equals(query_result_from_generator_second_element))

# BEGIN test
# Checks if a transaction is being performed correcty, in an atomic way. If not, 
# checks if a rollback is properly performed
def test_begin(db_context):
    df_dest = pandas.DataFrame({'a': [5, 8], 'b':[None, None], 'c':[None, None]})
    df_source = pandas.DataFrame({'a': [1, 2, 4, 6], 'b': [2, 5, 8, 9], 'c': [3, 6, 7, 9]})
    try:
        with db.begin() as conn:
            source_table_name = "test_table_1"
            dest_table_name = "test_table_2"
            conn.create_table(source_table_name, df_source)
            conn.create_table(dest_table_name, df_source)
            conn.update_table(source_table_name, dest_table_name, key="a", key_dir="<")   
    except:
        query_result_to_check_rollback = db.query("select name from sqlite_schema where type='table'")
        query_result_to_check_rollback2 = db.query("select * from test_table_1")
