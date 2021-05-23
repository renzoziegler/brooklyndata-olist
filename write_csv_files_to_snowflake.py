import pandas as pd
from pandas import errors


def read_csv_file(csv_file):
    """ Function that reads a csv file using Pandas read_csv function
    Parameters:
    csv_file: fully qualified path from CSV file
    Returns Pandas DataFrame with CSV File content

    Notes:
    - This function reads csv file and returns the dataframe. Any data transformation should be done within it
    - Currently there is no transformation being made. Considering that csv file columns names match table
    column names, and all csv files contains the same columns names, and each one contains all columns
    - If there is any difference among csv files, on structure, data type, etc, we should check it before
    trying to concatenate in a single dataframe

    """
    print('Reading file {}'.format(csv_file))
    try:
        # Using pandas read_csv function - Header is at first line (line 0)
        csv_file_dataframe = pd.read_csv(csv_file, header = 0)
    except FileNotFoundError:
        print('File {} not found'.format(csv_file))
    except errors.EmptyDataError:
        print('CSV File {} with empty data or header'.format(csv_file))
    except errors.ParserError:
        print('Parse Error when reading CSV File {}'.format(csv_file))
    except Exception:
        print('Error reading CSV File {}'.format(csv_file))
    
    return csv_file_dataframe

def write_csv_to_snowflake(engine, table_name, csv_file_paths, chunksize = 1000):
    """ Function that csv files into a table
    Parameters:
    engine: SQL Alchemy engine from create_engine function, with a connection to the target database
    table_name: fully qualified name of the table
    csv_file_path: a list of CSV file paths, fully qualified path for each file
    chunksize: number of rows in each batch to be written at a time. Default value: 1000 rows
    Returns true if success inserting ALL files, or raise an exception if there is an error reading any of the files
    or if there is some issue trying to insert result to table

    Notes:
    - I considered that all csv files have the same structure, and the same columns. If not, it will raise an exception
    when trying to concatenate data frames
    - If we should consider any possible difference, we should treat it in read_csv_file function
    - In order to improve database connection, there is only one insert, from all csv files at once. Should it take 
    much time, or we get a timeout, we should tune chunksize parameter, or even consider insert one csv file at a time

    """
    if not csv_file_paths:
        # Check if list is empty, raise exception if so
        raise ValueError("CSV File Paths list is empty")

    data_frames = [read_csv_file(csv_file) for csv_file in csv_file_paths]
    full_dataframe = pd.concat(data_frames)

    print('Writing files to table {}'.format(table_name))
    try:
        # Using to_sql function to insert it
        # Considering that table already exists, and table columns names match with csv columns names
        data_frames.to_sql(table_name, con = engine, if_exists='append', chunksize = chunksize)
    except ConnectionError:
        print("Unable to connect to database!")
    except Exception:
        print('Error writing CSV Files')

    return True


