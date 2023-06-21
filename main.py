import os, json
import superset_queries as sq
from db_conn import Postgresconn
import configurations as conf
# from flask import Flask, request, Response
# import google.cloud.logging
import functions_framework
import logging

# app = Flask(__name__)
# logging_client = google.cloud.logging.Client()
# log_name = "sidecheck"
# logger = logging_client.logger(log_name)

# @app.route("/api/dataset", methods = ['POST'])
@functions_framework.http
def main(request):
    if request.method == "POST":
        try:
            req_body = request.get_json()
            logging.warning(str(req_body))
            logging.warning(req_body["eric"])
            
        
            # if(not os.path.isdir(conf.csv_dir)):
            #     print(f'output folder {conf.csv_dir} not found ')
            #     exit()
            

            # # upload csv in superset 
            # csv_dir = f'{conf.csv_dir}';
            # files = [f for f in os.listdir(csv_dir) if os.path.isfile(os.path.join(csv_dir, f))]

            # for file in files: 
            #     ext = os.path.splitext(file)[1]
            #     file_name = os.path.splitext(file)[0].lower()
            #     if(conf.csv_ext == ext.lower()):


            '''
            To import a CSV dataset in Superset:
            - Create a new table in the chosen database with data from the CSV.
            - Insert the data into the newly created table.
            - Insert the required fields into the Superset table called "tables", which describes the newly created table in general.
            - Insert the description of each column of the newly created table into the Superset table called "table_columns". A field must be inserted for each column!
            '''

            superset_db = Postgresconn(conf.superset_db,conf.user,conf.password,conf.host,conf.port)
            query =  sq.get_database_info(conf.target_db)


            try:
                superset_db.connect()
                logging.warning(str(dir(superset_db)))
                logging.warning(str(dir(superset_db.conn)))
                logging.warning("about to log query:")
                logging.warning(query)
                logging.warning("connection status: " + str(superset_db.conn.status))
                
                logging.warning("about to call get_data_as_dict_norb()")
                target_db_info = superset_db.get_data_as_dict_norb(query);
                logging.warning("about to log target_db_info:")
                logging.warning(str(target_db_info))    

                real_target_db_name = target_db_info[0]['sqlalchemy_uri'].split('/')[-1]
                logging.warning("about to log real_target_db_name:")
                logging.warning(str(real_target_db_name))   

                if(real_target_db_name == conf.target_db):
                    target_db = Postgresconn(conf.target_db,conf.user,conf.password,conf.host,conf.port)
                    target_db.connect()
                elif(real_target_db_name == conf.superset_db):
                    target_db = superset_db
                elif conf.target_db.lower().replace(" ", "_") == real_target_db_name:
                    target_db = Postgresconn(conf.target_db.lower().replace(" ", "_"),conf.user,conf.password,conf.host,conf.port)
                    target_db.connect()
                else:
                    raise Exception("no match db found")


                # # recovering target database id
                logging.warning("about to get DB info")
                query = sq.get_database_info(conf.target_db)
                logging.warning("about to get target DB ID")
                target_db_id = superset_db.get_data_as_dict_norb(query)[0]['id']

                logging.warning("target_db_id: " + str(target_db_id) )

                # if(conf.replace_table_if_exists):
                #     query  = sq.get_id_from_tables(file_name)
                #     result = superset_db.get_data_as_dict_norb(query)
                    
                #     if (result != []): # if table is not not dropped in superset
                
                #         table_id = result[0]['id'];
                
                #         query = sq.delete_columns_from_superset_table_columns(table_id)
                #         superset_db.query_norb(query)

                #         query = sq.delete_column_from_superset_tables(table_id)
                #         superset_db.query_norb(query);
                        
                    
                #     # dropping target table
                #     query = sq.drop_table_if_exists(file_name)
                #     target_db.query_norb(query)
                    
                dataset_name = req_body["dataset_name"]
                dataset = req_body["dataset"]
                create_table = sq.csv_create_table_query(dataset_name, dataset);
                logging.warning("about to log create_tables query:")
                logging.warning(create_table)
                target_db.create_norb(create_table)
                logging.warning("just created table!")


                # insert_data = sq.csv_insert_data_query(file_name,csv_dir)
                insert_data = sq.insert_data_query(dataset_name, dataset)
                logging.warning("about to log insert_data query:")
                logging.warning(insert_data)
                target_db.insert_norb(insert_data)

                # superset_tables = sq.generate_superset_tables_query(table_name=file_name,database_id=target_db_id);
                logging.warning("about to attempt to build 'tables' query")
                superset_table_query = sq.generate_superset_tables_query(table_name=dataset_name, database_id=target_db_id);
                logging.warning("logging superset_tables:")
                logging.warning(superset_table_query)
                table_id = superset_db.insert_norb(superset_table_query);

            
                # superset_table_columns = sq.generate_superset_table_columns_query(csv_dir,file_name,table_id)
                superset_table_columns = sq.generate_superset_table_columns_query(dataset, table_id)

                logging.warning("about to log columns query:")
                logging.warning(superset_table_columns)
                superset_db.insert_norb(superset_table_columns)


                # superset_db.commit()
                # target_db.commit()

                # target_db.disconnect()
                # superset_db.disconnect()
                return "DB connect successful!"
            except Exception as e:
                # if(superset_db != target_db):
                #     target_db.rollback()
                # superset_db.rollback()
                # print('executed rollback')
                # print(f"some error occurred \n -> {e}")     
                logging.error(e.__str__())
                return "DB config exception!!"
        except Exception as e:
            logging.warning(e.__str__())

            return "Oof! A goof happened!!"

         
        
# upload_csv_in_superset()