import csv, logging, uuid
import psycopg2
import configurations as conf


superset_tables = 'tables'
superset_table_columns = 'table_columns'

# check if table exists
def table_exists_query(schema, table):
    return f"SELECT EXISTS ( SELECT FROM pg_tables WHERE schemaname = '{schema}' AND tablename  = '{table}');"

def drop_table_if_exists(table):
    return f'DROP TABLE IF EXISTS {table};'

def get_id_from_tables(table_name):
    return f"select id from {superset_tables} where table_name='{table_name}'" 

def delete_column_from_superset_tables(table_id):
    return f"delete from {superset_tables} where id={table_id}"

def delete_columns_from_superset_table_columns(table_id):
    return f"delete from {superset_table_columns} where table_id={table_id}"

def get_database_info(database_name):
    return  f"select * from dbs where database_name='{database_name}'"

column_list = []



# generate create table query based on csv (csv must contains titles)
def csv_create_table_query(dataset_name: str, dataset: list[object]):
    create_table_query = f' CREATE TABLE {dataset_name} ( \n';
    # csv_file=f'{dir}/{file_name}{conf.csv_ext}'

    # with open(csv_file, 'r') as file:
        # reader = csv.reader(file)
    
    # colum_names = next(reader)
    # data = next(reader) # first data column is used to infer types
    # data = dataset[0];

    # for j,col in enumerate(colum_names, start=0):
    create_table_query += "id serial primary key,"
    for row in dataset:
        for key, value in row.items():
            logging.warning("about to infer type for: " + str(value))
            data_type = infer_data_types(value)['postgres']
            logging.warning("done inferring type")
            create_table_query += f'{key} {data_type},\n'
        break    
        
    
    create_table_query = create_table_query[:-2]
    create_table_query += ');'
    
    return create_table_query;
        


# generate insert insert query based on csv data     
# def csv_insert_data_query(file_name: str, dir: str):
    # csv_file=f'{dir}/{file_name}{conf.csv_ext}'
    
    # with open(csv_file, 'r') as f:
    #     reader = csv.reader(f)
    #     column_list_string = ','.join(f'{ w }' for w in next(reader)) # getting column names
    #     insert_query = f'INSERT INTO {file_name} ( {column_list_string} ) VALUES \n' # starting insert query
    #     # insert query generation 
       
    #     for i,row in enumerate(reader,start=1):
    #         insert_query = f'{insert_query} ('                   
    #         for j, col in enumerate(row, start=1):              
    #             col = col.replace("'","")                       # replacing ' with nothing, ' cause problesm to the query
    #             try:        
    #                 float(col)                                  # trying to convert in float, because if value is not a number must be escaped with -> ''
    #             except:
    #                 if(col == ''):                              # if value is '' (empty string) 
    #                     col = 'NULL'                            # must add NULL value or postgres return error
    #                 else:                                       
    #                     col = f"'{col}'"                        # if it's string, simply add quotes 
    #             if(j == len(row)):
    #                 insert_query =  insert_query + col
    #             else: insert_query = insert_query + col + ','   # add comma only if is not last element 
    #         insert_query = f'{insert_query} ),\n'                # closing element parethesis and adding new line
    
    # # removing last comma
    # insert_query = insert_query[:-2] 
    # return  insert_query
    
def insert_data_query(dataset_name: str, dataset: list[object]):
    # csv_file=f'{dir}/{file_name}{conf.csv_ext}'
    
    # with open(csv_file, 'r') as f:
        # reader = csv.reader(f)
    #     column_list_string = ','.join(f'{ w }' for w in next(reader)) # getting column names
    column_list_string = ""
    for key, value in dataset[0].items():
        column_list.append(key)
        column_list_string += f"{key},"
    column_list_string = column_list_string[:-1] # shave off last comma

    #     insert_query = f'INSERT INTO {file_name} ( {column_list_string} ) VALUES \n' # starting insert query
    insert_query = f'INSERT INTO {dataset_name} ( {column_list_string} ) VALUES \n' # starting insert query
    
    # insert query generation
    for row in dataset:
        insert_query += "("
        for key, value in row.items():
            if type(value) == str:
                insert_query += f"'{value}',"
            elif value == '':
                insert_query += "null,"
            else:
                insert_query += f"{value},"
        insert_query = insert_query[:-1] + "),"
    insert_query = insert_query[:-1] + " RETURNING ID;"

    return insert_query
    
    # for i,row in enumerate(reader,start=1):
    #     insert_query = f'{insert_query} ('                   
    #     for j, col in enumerate(row, start=1):              
    #         col = col.replace("'","")                       # replacing ' with nothing, ' cause problesm to the query
    #         try:        
    #             float(col)                                  # trying to convert in float, because if value is not a number must be escaped with -> ''
    #         except:
    #             if(col == ''):                              # if value is '' (empty string) 
    #                 col = 'NULL'                            # must add NULL value or postgres return error
    #             else:                                       
    #                 col = f"'{col}'"                        # if it's string, simply add quotes 
    #         if(j == len(row)):
    #             insert_query =  insert_query + col
    #         else: insert_query = insert_query + col + ','   # add comma only if is not last element 
    #     insert_query = f'{insert_query} ),\n'                # closing element parethesis and adding new line



    #     for i,row in enumerate(reader,start=1):
    #         insert_query = f'{insert_query} ('                   
    #         for j, col in enumerate(row, start=1):              
    #             col = col.replace("'","")                       # replacing ' with nothing, ' cause problesm to the query
    #             try:        
    #                 float(col)                                  # trying to convert in float, because if value is not a number must be escaped with -> ''
    #             except:
    #                 if(col == ''):                              # if value is '' (empty string) 
    #                     col = 'NULL'                            # must add NULL value or postgres return error
    #                 else:                                       
    #                     col = f"'{col}'"                        # if it's string, simply add quotes 
    #             if(j == len(row)):
    #                 insert_query =  insert_query + col
    #             else: insert_query = insert_query + col + ','   # add comma only if is not last element 
    #         insert_query = f'{insert_query} ),\n'                # closing element parethesis and adding new line
    
    # # removing last comma
    # insert_query = insert_query[:-2] 
    # return  insert_query
    return


# generate query for table 'tables' in superset this table describe the table you wanto to create
def generate_superset_tables_query(table_name:str,database_id:int):
    
    superset_table = 'tables'
 
    cols_dict = conf.get_superset_tables_conf(table_name,database_id,database_id,str(uuid.uuid4()))

    columns_query = f'INSERT INTO {superset_table} (\n' 
    values_query = 'VALUES (\n';

    for key in cols_dict:       
        columns_query += f'{key}, \n' # adding columns name
        
        # adding values                      
        if(cols_dict[key] is not None):                            
            values_query += f'{str(cols_dict[key])}, \n' 
        else: values_query += 'NULL ,\n'

    columns_query = columns_query[:-3] + ')'  # removing last comma
    values_query = values_query[:-3] + ')'  # removing last comma

    return( columns_query +  '\n' + values_query + ' RETURNING id;')
    

    

# generate query for table 'table_columns', in superset this table describe each columns of the created table
# def generate_superset_table_columns_query(dir:str , file_name: str, table_id):
def generate_superset_table_columns_query(dataset: list[object], table_id):

    superset_table = 'table_columns';

    column_names = conf.superset_table_columns_cols;

    query = f'INSERT INTO {superset_table} ( ' + ', '.join(f'{ w }' for w in column_names) + ' ) VALUES \n' 

    # csv_file=f'{dir}/{file_name}{conf.csv_ext}'

    # with open(csv_file, 'r') as f:
        # reader = csv.reader(f)
    
        # colum_names = next(reader)
        # data = next(reader) # first data column is used to infer types

    # for j,col in enumerate(column_list, start=0):
    for col, value in dataset[0].items():
        query += '('     
        data_type = infer_data_types(dataset[0][col])['superset'] 
        values_dict = conf.get_superset_table_columns_conf(table_id, col, data_type,str(uuid.uuid4()))
        for key in values_dict:
            if(values_dict[key] is not None): # se l'elemento è None non deve venire stampato come stringa ma come NULL
                query += f'{str(values_dict[key])} , ' 
            else: query += 'NULL ,'
        query = query[:-2]  # removing last element comma;
        query += '),\n' 
    query = query[:-2] # removing last row comma

    return(query)



def infer_data_types(col):
    db_type = {}
    if type(col) == int:
        db_type['postgres'] = 'int'
        db_type['superset'] = 'BIGINT'
        return db_type
    elif type(col) == float:
        db_type['postgres'] = 'float'
        db_type['superset'] = 'DOUBLE PRECISION'
        return db_type
    else:
        db_type['postgres'] = 'varchar'
        db_type['superset'] = 'VARCHAR'
        return db_type
        
    # try:
    #     int(col)
    #     type['postgres'] = 'int'
    #     type['superset'] = 'BIGINT'
    #     return type
    # except:
    #     try:
    #         float(col)
    #         type['postgres'] = 'float'
    #         type['superset'] = 'DOUBLE PRECISION'
    #         return type
    #     except:
    #         type['postgres'] = 'varchar'
    #         type['superset'] = 'VARCHAR'
    #         return type
            