import boto3
import json
import elasticsearch
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import sys 
import argparse
import logging
from logging.handlers import RotatingFileHandler
from importlib import import_module

def config_logging ():
    """
    logging configuration function to print in the log files in the command line
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')
    
    # configuration of the logging in the log file
    file_handler = RotatingFileHandler('logs/migration_s3_elasticsearch.log', 'a', 1000000, 1)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # configuration of the logging in the terminal
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    
    return logger
    
def parse_args(args):
    """
        parses the command line arguments. 
        Non provided arguments have a default value in the config file.  
    """
    parser = argparse.ArgumentParser()

    parser.add_argument("--config-file", help="configuration file, default :config.config_migration_s3_elasticsearch ", type=str, default="config.config_migration_s3_elasticsearch", action="store")
    
    parser.add_argument("--host", help="host, default value in config-file ", type=str, default=None, action="store")

    parser.add_argument("--port", help="port, default value in config-file ", type=int, default=None, action="store")
    
    parser.add_argument("--index_name", help="index_name, default value in config-file ", type=str, default=None, action="store")
    
    parser.add_argument("--bucket_name", help="bucket_name, default value in config-file ", type=str, default=None, action="store")
    
    parser.add_argument("--bulk_size", help="bulk_size, default value in config-file ", type=int, default=None, action="store")
    
    parser.add_argument("--progress_file", help="progress_file, default value in config-file ", type=str, default=None, action="store")
    
    parser.add_argument("--new", help="""to create new es database : migration_s3_elasticsearch.py --new 1 
    to  continue from the last execution : migration_s3_elasticsearch.py --new 0
    default value in config-file""",
                        type=int, default=None, action='store'
                        )

    return parser.parse_args(args)

def creation_index (host, port, index_name, new, progress_file, logger):
    """
    script to create the elasticsearch index if the variable new = 1 or the index_name does not exist
    """
    es = Elasticsearch([{'host': host, 'port': port}])
    
    if new : # create a new index
        if es.indices.exists(index_name): # if index_name exists delete it to create a new one 
            logger.info("deleting '%s' index..." % (index_name))
            res = es.indices.delete(index = index_name)
            logger.info(" response: '%s'" % (res))   
        # new index creation 
        logger.info("creating '%s' index..." % (index_name))
        res = es.indices.create(index=index_name) 
        logger.info(" response: '%s'" % (res))
        
        # initialize the progress_file 
        with open(progress_file,"w") as p:
            save_progress = {"position_file":0}
            json.dump(save_progress, p)
        
    else : # create a index only if index_name does not exists
        if not(es.indices.exists(index_name)): # if the index_name does not exists create one
            logger.info("creating '%s' index..." % (index_name))
            res = es.indices.create(index=index_name)
            logger.info(" response: '%s'" % (res))
            
            # initialize the progress_file
            with open(progress_file,"w") as p:
                save_progress = {"position_file":0}
                json.dump(save_progress, p)
            
    
def migration_s3_es (host, port, index_name, bucket_name, bulk_size, progress_file ,logger) : 
    """
    script to migrate the json data stored in amazon s3 to elasticsearch index_name
    """
    es = Elasticsearch([{'host': host, 'port': port}])   
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name) # s3 bucket
    objects = bucket.objects.all() # objects in the s3 bucket 'bucket_name'
    
    # getting the position_file in the progress_file to continue from the last execution if position_file != 0
    try:
        with open(progress_file,"r") as p:
            position_file = json.load(p)["position_file"]
            logger.info(" start loading from position '%d' " % (position_file))
    except IOError:
        logger.warring(" Progress file not accessible. Starting migration from beginning")
        position_file = 0
    

    bulk_data = [] #initialize the block that will be ingested in elasticsearch index
    i=0
    logger.info(" loading data from amazon s3 ")
    for obj in objects: # iterate over the s3 objects
        i=i+1
        if i > position_file: # condition to continue from the last execution
            object_key = obj.key # get the object key
            s3_object = s3.Object(bucket_name, object_key) # get the s3 objet with the bucket_name and the object_key
            data_string = s3_object.get()['Body'].read().decode('utf-8') # download the s3 object in the data_string

            data_dict = json.loads(data_string) #convert string to dict (json)
            op_dict = {"index":{
                    "_index" : index_name,
                    "_id" : data_dict["docID"]       
            }} # assign the open dict contains the index_name and the id of the document 
            
            bulk_data.append(op_dict)
            bulk_data.append(data_dict)

            if len(bulk_data)>=bulk_size : # if the bulk_data list arrive to the bulk_size insert data in the index
                logger.info("inserting data in '%s' index..." % (index_name))
                res = es.bulk(index = index_name, body = bulk_data, refresh = True)
                logger.debug(" response: '%s'" % (res))
                logger.info(" '%d' document inserted" % (i))
                bulk_data=[] #reinitialize the bluk_data to empty list
                
                # save/update the position in the progress_file 
                with open(progress_file,"w") as p:
                    save_progress = {"position_file":i}
                    json.dump(save_progress, p)
                    
                logger.info(" loading data from amazon s3 ")
                
    if len(bulk_data)!= 0 : # insert the data that remains in the bulk_data
        logger.info("inserting data in '%s' index..." % (index_name))
        res = es.bulk(index = index_name, body = bulk_data, refresh = True)
        logger.debug(" response: '%s'" % (res))
        logger.info(" '%d' document inserted" % (i))
        
        with open(progress_file,"w") as p:
            save_progress = {"position_file":i}
            json.dump(save_progress, p)

    
def main(args):
    
    args = parse_args(args)
    logger = config_logging()
    config = import_module(args.config_file)
    
    if args.host is None : 
        args.host= config.host
    if args.port is None : 
        args.port= config.port
    if args.index_name is None : 
        args.index_name= config.index_name
    if args.bucket_name is None : 
        args.bucket_name= config.bucket_name
    if args.bulk_size is None : 
        args.bulk_size= config.bulk_size
    if args.progress_file is None : 
        args.progress_file= config.progress_file
    if args.new is None:
        args.new = config.new
        
    creation_index (args.host, args.port, args.index_name, args.new, args.progress_file, logger)
    
    migration_s3_es(args.host, args.port, args.index_name, args.bucket_name, args.bulk_size, args.progress_file, logger)

if __name__ == '__main__':
    main(sys.argv[1:])

