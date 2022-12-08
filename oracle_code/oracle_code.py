'''
Oracle helper functions - not all functions may be used by a parent script.

Note that all DDL statements auto-commit even if the transaction is rolled back. 
This includes ALTER, CREATE, DROP, RENAME, TRUNCATE, etc., so avoid those statements!
See https://docs.oracle.com/cd/B14117_01/server.101/b10759/statements_1001.htm#i2099120
'''
import cx_Oracle
import sys
import petl as etl
import numpy as np
import pandas as pd
import time

BATCH_SIZE = 20000

def connect_to_db(db_creds): 
    '''
    Return an Oracle Connection using a JSON dictionary of credentials
    '''
    dsn = cx_Oracle.makedsn(
        host=db_creds['dsn']['host'], 
        port=db_creds['dsn']['port'], 
        service_name=db_creds['dsn']['service_name'])
    conn = cx_Oracle.connect(
        user=db_creds['username'], 
        password=db_creds['password'], 
        dsn=dsn)
    return conn

def commit_transactions(db_conn: cx_Oracle.Connection, commit: bool): 
    '''
    Commit all transactions if commit = True, rollback otherwise
    '''
    if commit: 
        db_conn.commit()
    else: 
        db_conn.rollback()
    print(f'All database transactions were {"COMMITTED" if commit else "ROLLED BACK"}')

def append_petl(all_data, db_conn: cx_Oracle.Connection, table: str, truncate: bool): 
    '''
    Format PETL data by first coercing to Pandas dataframe, then append to table
    '''
    print('Starting timer')
    start = time.time()
    # etl.todataframe() is 2x faster than list(etl.records()) and 6x faster than looping with etl.rowslice(), which got worse with time
    df = etl.todataframe(all_data)
    print(f'    time: {time.time() - start:,.1f} seconds')
    append_df(df, db_conn, table, truncate)
    print(f'Finished in {time.time() - start:,.1f} seconds')

def append_df(df: pd.DataFrame, db_conn: cx_Oracle.Connection, table: str, truncate: bool): 
    '''
    Format a pandas dataframe and append to table
    '''
    all_data = format_data(df, db_conn, table) # Returns as list/records
    append_data(all_data, db_conn, table, truncate)

def update_join(df: pd.DataFrame, db_conn: cx_Oracle.Connection, table: str, join_keys: list, commit: bool): 
    '''
    Updates the table "table" with data df by joining on join_keys and committing if 
    commit==True via the following SQL steps: 

    1. Creates an empty temporary table using the schema of table, failing if temp 
        table already exists
    2. Adds join_keys as Primary Keys (necessary to avoid Non-Key Preserved Table error) 
    3. Appends df to temp_table
    4. Updates data in table by inner joining temp table using join_keys
    5. Deletes the data utilized in step 4 from temp table
    6. Appends any data remaining in temp table to table
    7. Drops the temp table

    Note that the CREATE TABLE and DROP TABLE statements auto-commit, so to take effect 
    the commit cannot wait until the end of the parent script.
    '''
    temp_table = "TEMP_" + table
    statement_create = f'''
        CREATE TABLE {temp_table}
        AS (
            SELECT * 
            FROM {table}
            WHERE 1=0
            )
    '''
    statement_add_pk = f'''
        ALTER TABLE {temp_table}
        ADD CONSTRAINT {temp_table}_PK PRIMARY KEY ({', '.join(join_keys)})
        ENABLE
    '''
    with db_conn.cursor() as cur:
        print(f'Creating new table {temp_table}')
        cur.execute(statement_create)
        print(f'Adding primary key(s) {join_keys} to {temp_table}')
        cur.execute(statement_add_pk)
    
    append_df(df, db_conn, temp_table, False)

    old_cols = [
        table + '.' + col + ' AS OLD_' + col # e.g. "HOMESTEAD_EXEMPTIONS.PROPERTY_ID AS OLD_PROPERTY_ID"
        for col in df.columns]
    new_cols = [
        temp_table + '.' + col + ' AS NEW_' + col # e.g. "TEMP_HOMESTEAD_EXEMPTIONS.PIN_NUMBER AS NEW_PIN_NUMBER"
        for col in df.columns]
    cols_select = ', '.join(old_cols + new_cols)
    join_cols = [
        table + '.' + col + ' = ' + temp_table + '.' + col # e.g. "HOMESTEAD_EXEMPTIONS.PIN_NUMBER = TEMP_HOMESTEAD_EXEMPTIONS.PIN_NUMBER"
        for col in join_keys]
    cols_join = ' AND '.join(join_cols)
    set_cols = [
        'joined.OLD_' + col + ' = joined.NEW_' + col # e.g. "joined.OLD_OPA_NUMBER = joined.NEW_OPA_NUMBER"
        for col in df.columns if col not in join_keys]
    cols_set = ', '.join(set_cols)
    statement_update = f'''
    UPDATE (
        SELECT 
            {cols_select}
        FROM {table}
            INNER JOIN {temp_table} ON 
                {cols_join}
    ) joined
    SET 
        {cols_set}
    '''
    statement_delete_temp = f'''
    DELETE    
    FROM {temp_table}
    WHERE EXISTS (
        SELECT *
        FROM {table}
        WHERE 
            {cols_join}) 
    '''
    statement_append_from_temp = f'''
    INSERT INTO {table}
    SELECT * 
    FROM {temp_table} 
    '''
    with db_conn.cursor() as cur:
        print(f'Updating table {table}')
        cur.execute(statement_update)
        print(f'Successfully updated {cur.rowcount} rows in table "{table}"')
        cur.execute(statement_delete_temp)
        print(f'Successfully deleted {cur.rowcount} rows from temp table "{temp_table}"')
        cur.execute(statement_append_from_temp)
        print(f'Successfully appended {cur.rowcount} rows from temp table "{temp_table}" into table "{table}"')
        commit_transactions(db_conn, commit) # The commit must occur before the DROP TABLE statement to take effect
        print(f'Dropping temp table {temp_table}')
        cur.execute(f'DROP TABLE {temp_table}')

def format_data(df, db_conn: cx_Oracle.Connection, table: str, return_col_names:bool = False): 
    '''
    Takes a pandas dataframe and 
    1. Ensures column order exactly matches the database's, and are UPPERCASE
    2. Drops any columns not in the database table
    3. Returns the data as a list of tuples
    '''
    with db_conn.cursor() as cur:
        statement = f"SELECT * FROM {table} WHERE ROWNUM = 1"
        cur.execute(statement)
        desc = cur.description
        
    df.columns = [col.upper() for col in df.columns]

    oracle_order = []
    for oracle_col in desc: 
        if oracle_col[0] not in df.columns:
            raise(IndexError(f'Column "{oracle_col[0]}" from database table "{table}" not in dataframe'))
        oracle_order.append(oracle_col[0])
    
    df = df.reindex(columns=[col for col in oracle_order]) # Drops columns not in db table
    df = df.replace({
        '': None, 
        np.nan: None}).astype('object') # As type 'object' is needed for Nones
    data = list(df.to_records(index=False))
    
    if return_col_names: 
        return data, oracle_order
    return data

def truncate_data(db_conn: cx_Oracle.Connection, table: str): 
    '''
    Truncate all data from table
    '''
    execution_statement = f"DELETE FROM {table}" 
    with db_conn.cursor() as cur:
        cur.execute(execution_statement)
    print(f'Successfully deleted {cur.rowcount} rows from table "{table}"')

def append_data(all_data, db_conn: cx_Oracle.Connection, table: str, truncate: bool): 
    '''
    Prepares the APPEND query, truncates data if instructed, and batch appends data
    '''
    print(f'Appending to "{table}"')
    l = [x + 1 for x in list(range(len(all_data[0])))] # Length of the first record
    values = ', :'.join(map(str, l))
    if truncate: 
        truncate_data(db_conn, table)
    execution_statement = f"INSERT INTO {table} VALUES (:{values})"
    
    start_pos = 0
    while True:
        data = all_data[start_pos:start_pos + BATCH_SIZE]
        if data == []: 
            break
        print(f'    appending rows [{start_pos}:{len(data) - 1}]')
        start_pos += BATCH_SIZE
        with db_conn.cursor() as cur:
            try: 
                cur.executemany(execution_statement, data)
                print(f"Successfully appended {cur.rowcount} rows to table '{table}'")
            except Exception as e: 
                _print_data_error(execution_statement, data, e)

def delete_data(np_array, db_conn: cx_Oracle.Connection, table: str, column: str): 
    '''
    Runs a delete query where one column contains a delete key
    '''
    if len(np_array) != 0: 
        execution_statement = f"DELETE FROM {table} WHERE {column} = :1"
        data = list(zip(np_array))
        with db_conn.cursor() as cur:
            try: 
                cur.executemany(execution_statement, data)
                print(f"Successfully deleted {cur.rowcount} rows from table '{table}'")
            except Exception as e: 
                print('Execution Statement Failed:')
                print(f'{execution_statement}')
                raise e
    else: 
        print('Not deleting - Array has length 0')

def _print_data_error(execution_statement: str, data, e): 
    '''
    Internal function to print the execution statement, data head, data tail, and 
    exception raised by a query
    '''
    print('Execution Statement Failed:')
    print(f'{execution_statement}')
    print('data[0:5]:')
    for row in data[0:5]: 
        print(f'    {row}')
    print('data[-5:]:')
    for row in data[-5:]: 
        print(f'    {row}')
    print(e)
    sys.exit(1)