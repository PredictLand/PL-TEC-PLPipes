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


def extract_path(string):
    start_index = string.find("'") + 1
    end_index = string.rfind("'")
    if start_index != -1 and end_index != -1:
        path = string[start_index:end_index]
        return path
    else:
        return None

# Handling DB creation and destruction via a teporary file
@contextmanager
def handle_db():
    try:
        # TESTS SETUP
        work_dir = extract_path(str(tempfile.TemporaryDirectory()))
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

# CREATE TABLE and QUERY test
def test_create_query(db_context):
    db.create_table("test_table", df)
    query_result = db.query("select * from test_table")
    assert df.equals(query_result)

# READ TABLE test
def test_read(db_context):
    db.create_table("test_table", df)
    reading_result = db.read_table("test_table")
    assert df.equals(reading_result)

# EXECUTE test
def test_create_table_append(db_context):
    df1 = pandas.DataFrame({'a': [6], 'b': [7], 'c': [9]})
    df_expected = pandas.DataFrame({'a':[1, 4, 2, 6], 'b':[2, 5, 9, 7], 'c':[3, 6, 7, 9]})
    db.create_table("test_table", df)
    db.create_table("test_table", df1, if_exists="append")
    query_result = db.query("select * from test_table")
    assert df_expected.equals(query_result)
    
# COPY TABLE test
def test_copy_table_df(db_context):
    db.create_table("test_table", df)
    source_table_name = "test_table"
    dest_table_name = "test_table_copy"
    db.copy_table("test_table", "test_table_copy")
    query_result_orig_table = db.query("select * from test_table")
    query_result_copy_table = db.query("select * from test_table_copy")
    assert query_result_orig_table.equals(query_result_copy_table) 

# CREATE VIEW test
def test_create_view(db_context):
    db.create_table("test_table", df)
    view = db.create_view("test_view", "select * from test_table")
    query_result_table = db.query("select * from test_table")
    query_result_view = db.query("select * from test_view")
    assert query_result_table.equals(query_result_view)

# DELETE ROW test
def test_execute_delete(db_context):
    df_expected = pandas.DataFrame({'a':[4, 2], 'b':[5, 9], 'c':[6, 7]})
    db.create_table("test_table", df)
    db.execute("delete from test_table where a=1")
    query_result = db.query("select * from test_table")
    assert df_expected.equals(query_result)

# ALTER TABLE ADD test
def test_execute_alter_add(db_context):
    df_expected = pandas.DataFrame({'a':[1, 4, 2], 'b':[2, 5, 9], 'c':[3, 6, 7], 'd':[None, None, None]})
    db.create_table("test_table", df)
    db.execute("alter table test_table add d INT")
    query_result = db.query("select * from test_table")
    assert df_expected.equals(query_result)

# ALTER TABLE DROP test
def test_execute_alter_drop(db_context):
    df_expected = pandas.DataFrame({'a':[1, 4, 2], 'b':[2, 5, 9]})
    db.create_table("test_table", df)
    db.execute("alter table test_table drop c")
    query_result = db.query("select * from test_table")
    assert df_expected.equals(query_result)

# ALTER TABLE RENAME test
def test_execute_alter_rename(db_context):
    df_expected = pandas.DataFrame({'a':[1, 4, 2], 'e':[2, 5, 9], 'c':[3, 6, 7]})
    db.create_table("test_table", df)
    db.execute("alter table test_table rename column b to e")
    query_result = db.query("select * from test_table")
    assert df_expected.equals(query_result)


# DROP TABLE test
def test_execute_drop(db_context):
    db.create_table("test_table", df)
    db.create_table("test_table_to_drop", df)
    query_result_before_dropping = db.query("select name from sqlite_schema where type='table'")
    db.execute("drop table test_table_to_drop")
    query_result_after_dropping = db.query("select name from sqlite_schema where type='table'")
    equal_dataframes = query_result_before_dropping.equals(query_result_after_dropping)
    assert equal_dataframes == False
   

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
# Single chunk
def test_query_chunked_single_chunk(db_context):
    # df_to_chunk = pandas.DataFrame({'a':[1, 4, 2], 'b':[2, 5, 9], 'c':[3, 6, 7], 'd':[3, 6, 7], 'e':[3, 6, 7], 'f':[3, 6, 7], 'g':[3, 6, 7], 'h':[3, 6, 7], 'i':[3, 6, 7], 'j':[3, 6, 7]})
    df_to_chunk = pandas.DataFrame({'a':[1, 4, 2], 'b':[2, 5, 9], 'c':[3, 6, 7], 'd':[3, 6, 7]})
    df_expected = df_to_chunk
    #df_to_chunk = pandas.DataFrame({'a':[1, 4], 'b':[2, 5], 'c':[3, 6], 'd':[3, 6]})
    db.create_table("test_table", df_to_chunk)
    query_result = db.query_chunked("select * from test_table")
    query_result_from_generator = next(query_result)
    assert df_expected.equals(query_result_from_generator)

# 2 chunks
def test_query_chunked_2_as_chunksize(db_context):
    # df_to_chunk = pandas.DataFrame({'a':[1, 4, 2], 'b':[2, 5, 9], 'c':[3, 6, 7], 'd':[3, 6, 7], 'e':[3, 6, 7], 'f':[3, 6, 7], 'g':[3, 6, 7], 'h':[3, 6, 7], 'i':[3, 6, 7], 'j':[3, 6, 7]})
    df_to_chunk = pandas.DataFrame({'a':[1, 4, 2], 'b':[2, 5, 9], 'c':[3, 6, 7], 'd':[3, 6, 7]})
    df_expected = pandas.DataFrame({'a':[1, 4], 'b':[2, 5], 'c':[3, 6], 'd':[3, 6]})
    #df_to_chunk = pandas.DataFrame({'a':[1, 4], 'b':[2, 5], 'c':[3, 6], 'd':[3, 6]})
    db.create_table("test_table", df_to_chunk)
    query_result = db.query_chunked("select * from test_table", chunksize=2)
    query_result_from_generator = next(query_result)
    assert df_expected.equals(query_result_from_generator)
    # query_result_df = pandas.DataFrame(query_result_from_generator)
    # assert df_to_chunk.equals(query_result_df)

# 3 chunks
def test_query_chunked_3_as_chunksize(db_context):
    # df_to_chunk = pandas.DataFrame({'a':[1, 4, 2], 'b':[2, 5, 9], 'c':[3, 6, 7], 'd':[3, 6, 7], 'e':[3, 6, 7], 'f':[3, 6, 7], 'g':[3, 6, 7], 'h':[3, 6, 7], 'i':[3, 6, 7], 'j':[3, 6, 7]})
    df_to_chunk = pandas.DataFrame({'a':[1, 4, 2, 7, 8, 0], 'b':[2, 5, 9, 4, 2, 3], 'c':[3, 6, 7, 5, 7, 7], 'd':[3, 8, 8, 3, 6, 7]})
    df_expected = pandas.DataFrame({'a':[1, 4, 2], 'b':[2, 5, 9], 'c':[3, 6, 7], 'd':[3, 8, 8]})
    #df_to_chunk = pandas.DataFrame({'a':[1, 4], 'b':[2, 5], 'c':[3, 6], 'd':[3, 6]})
    db.create_table("test_table", df_to_chunk)
    query_result = db.query_chunked("select * from test_table", chunksize=3)
    query_result_from_generator = next(query_result)
    # query_result_df = pandas.DataFrame(query_result_from_generator)
    assert df_expected.equals(query_result_from_generator)


# QUERY GROUP test
def test_query_group(db_context):
    df_to_chunk = pandas.DataFrame({'a':[1, 4, 2], 'b':[2, 5, 7], 'c':[3, 3, 3], 'd':[3, 6, 7]})
    # df_expected = 
    db.create_table("test_table", df_to_chunk)
    query_result = db.query_group("select * from test_table", by=["b"])
    query_result_from_generator = next(query_result)
    # assert df_expected.equals(query_result_from_generator)

# BEGIN test
def test_begin(db_context):
    df_dest = pandas.DataFrame({'a': [5, 8], 'b':[None, None], 'c':[None, None]})
    df_source = pandas.DataFrame({'a': [1, 2, 4, 6], 'b': [2, 5, 8, 9], 'c': [3, 6, 7, 9]})
    
    try:
        with db.begin() as conn:
            source_table_name = "test_table_1"
            dest_table_name = "test_table_2"
            query_result_to_check_rollback = conn.query("select name from sqlite_schema where type='table'")
            # print(f"Tables before create:\n{query_result_to_check_rollback}")
            conn.create_table(source_table_name, df_source)
            query_result_to_check_rollback = conn.query("select name from sqlite_schema where type='table'")
            query_result_to_check_rollback2 = conn.query("select * from test_table_1")
            # print(f"Table content between both create:\n{query_result_to_check_rollback2}")
            # print(f"Tables between both create:\n{query_result_to_check_rollback}")
            conn.create_table(dest_table_name, df_source)
            query_result_to_check_rollback = conn.query("select name from sqlite_schema where type='table'")
            # print(f"Tables after create:\n{query_result_to_check_rollback}")
            conn.update_table(source_table_name, dest_table_name, key="a", key_dir="<")   
    except:
        query_result_to_check_rollback = db.query("select name from sqlite_schema where type='table'")
        # print(f"Tables when except:\n{query_result_to_check_rollback}")
        query_result_to_check_rollback2 = db.query("select * from test_table_1")
        # print(f"Table content when except:\n{query_result_to_check_rollback2}")
        


