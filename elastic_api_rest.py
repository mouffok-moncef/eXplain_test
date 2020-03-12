from flask import Flask, request
from flask_restful import Resource, Api, reqparse
from elasticsearch import Elasticsearch
import json
import sys
import logging
from logging.handlers import RotatingFileHandler
from importlib import import_module
import argparse


class search_api(Resource): # the class for the search ressource 
    def get(self):
        # arguments recovery of the get api of the search ressource 
        args = parser_search.parse_args()
        source_name = args["sourceName"]
        ingestionDate_eq = args["ingestionDate_eq"]
        ingestionDate_lt = args["ingestionDate_lt"]
        ingestionDate_gt = args["ingestionDate_gt"]
        topics = args["topics"]
        namedEntities = args["namedEntities"]
        locations = args["locations"]
        
        # elasticsearch query construction
        must_list = []
        if source_name is not None : 
            must_list.append({"match" : {"sourceName.keyword": source_name}})
        if ingestionDate_eq is not None : 
            must_list.append({"match" : {"ingestionDate": ingestionDate_eq}})
        if ingestionDate_lt is not None : 
            must_list.append({"range" : {"ingestionDate": {"lt" :ingestionDate_lt}}})
        if ingestionDate_gt is not None : 
            if ingestionDate_lt is not None : 
                must_list[-1]['range']['ingestionDate']['gt'] = ingestionDate_gt
            else :
                must_list.append({"range" : {"ingestionDate": {"gt" :ingestionDate_gt}}})
        if topics is not None : 
            must_list.append({"terms" : {"topics.name.keyword": topics}})
        if namedEntities is not None : 
            must_list.append({"exists" : {"field": "namedEntities"}})
        if locations is not None : 
            must_list.append({"terms" : {"locations.name.keyword": locations}})
            
        body = {
          "stored_fields": [], "size":size,
            "query": {
              "bool": {
                "must": must_list
              }
            }
        }
        logger.debug(body)
        
        # query execution
        res = es.search(index=index_name, body = body ) 
        # get the ids of the results
        list_id = list (map (lambda x : x["_id"], res["hits"]["hits"]))
        
        return list_id
    
class group_count_api(Resource): # the class for the group ressource 
    def get(self):
        # arguments recovery of the get api of the group ressource
        args = parser_count.parse_args()
        isSpamProb = args["isSpamProb"]
        
        # elasticsearch query construction
        body = {
            "size": 0,
            "query": {
                "range" : {
                    "isSpamProb": {
                        "lt" : isSpamProb
                    }
                }
            },
            "aggs" : {
                "nb_article_by_publicationDate" : {
                    "terms" : { "size":50, "field" : "publicationDate" } 
                }
            }
        }        
        
        # query execution
        res = es.search (index=index_name, body= body)
        list_count = res["aggregations"]
        
        return list_count

def config_logging ():
    """
    logging configuration function to print in the log files in the command line
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)     
    formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')
    
    # configuration of the logging in the log file
    file_handler = RotatingFileHandler('logs/elastic_api_rest.log', 'a', 1000000, 1)
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

    parser.add_argument("--config-file", help="configuration file, default :config.config_elastic_api_rest ", type=str, default="config.config_elastic_api_rest", action="store")
    
    parser.add_argument("--host", help="host, default value in config-file ", type=str, default=None, action="store")

    parser.add_argument("--port", help="port, default value in config-file ", type=int, default=None, action="store")
    
    parser.add_argument("--index_name", help="index_name, default value in config-file ", type=str, default=None, action="store")
    
    parser.add_argument("--size", help="size, default value in config-file ", type=int, default=None, action="store")
    
    return parser.parse_args(args)

def get_api_parsers():
    """
        function to the argument parser for the api rest
    """
    # the argument parser for the search ressource
    parser_search = reqparse.RequestParser()
    parser_search.add_argument('sourceName', type=str, help="source name", default=None)
    parser_search.add_argument('ingestionDate_eq', type=str, help="date d'ingestion exacte", default=None)
    parser_search.add_argument('ingestionDate_lt', type=str, help="date d'ingestion inférieure à", default=None)
    parser_search.add_argument('ingestionDate_gt', type=str, help="date d'ingestion supérieure à", default=None)
    parser_search.add_argument('topics', action="append", type=str,  help="list of topics", default=None)
    parser_search.add_argument('namedEntities', type=bool, help="où une entité nommée (namedEntities) apparait", default=None)
    parser_search.add_argument('locations', action="append", type=str, help="territoires détéctés", default=None)
    # the argument parser for the group ressource
    parser_count = reqparse.RequestParser()
    parser_count.add_argument('isSpamProb', type=float, help="give the spam prob threshold", default=0.1)
    return parser_search, parser_count


if __name__ == '__main__':
    
    args = parse_args(sys.argv[1:])
    logger = config_logging()
    config = import_module(args.config_file)
    if args.host is None : 
        args.host= config.host
    if args.port is None : 
        args.port= config.port
    if args.index_name is None : 
        args.index_name= config.index_name
    if args.size is None : 
        args.size= config.size
        
    es = Elasticsearch([{'host': args.host, 'port': args.port}])
    index_name = args.index_name
    size = args.size
    
    parser_search, parser_count = get_api_parsers()

    app = Flask(__name__)
    api = Api(app)

    api.add_resource(search_api, '/search')
    api.add_resource(group_count_api, '/group')
    app.run(debug=True)
