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
    '''
        To import a CSV dataset in Superset:
        - Create a new table in the chosen database with data from the CSV.
        - Insert the data into the newly created table.
        - Insert the required fields into the Superset table called "tables", which describes the newly created table in general.
        - Insert the description of each column of the newly created table into the Superset table called "table_columns". A field must be inserted for each column!
    '''

    if request.method == "POST":
        try:
            req_body = request.get_json()
            superset_db = Postgresconn(conf.superset_db,conf.user,conf.password,conf.host,conf.port)
            query =  sq.get_database_info(conf.target_db)


            try:
                superset_db.connect()
                target_db_info = superset_db.get_data_as_dict_norb(query);
                real_target_db_name = target_db_info[0]['sqlalchemy_uri'].split('/')[-1]

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

                # recovering target database id
                query = sq.get_database_info(conf.target_db)
                target_db_id = superset_db.get_data_as_dict_norb(query)[0]['id']

                dataset_name = req_body["dataset_name"]
                dataset = req_body["dataset"]
                create_table = sq.csv_create_table_query(dataset_name, dataset);
                target_db.create_norb(create_table)

                insert_data = sq.insert_data_query(dataset_name, dataset)
                target_db.insert_norb(insert_data)

                superset_table_query = sq.generate_superset_tables_query(table_name=dataset_name, database_id=target_db_id);
                table_id = superset_db.insert_norb(superset_table_query);

                superset_table_columns = sq.generate_superset_table_columns_query(dataset, table_id)
                superset_db.insert_norb(superset_table_columns)

                return "DB connect successful!"
            except Exception as e:
                logging.error(e.__str__())
                return "DB config exception!!"
        except Exception as e:
            logging.warning(e.__str__())
            return "An exception occurred while trying to update the database!"
    else:
        return "Only POST requests are allowed!"
