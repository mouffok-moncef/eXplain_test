#!/usr/bin/env python
# coding: utf-8

from requests import get
import logging
from logging.handlers import RotatingFileHandler
from pprint import pformat
from requests.exceptions import ConnectionError


def config_logging ():
    """
    logging configuration function to print in the log files in the command line
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)     
    formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')
    
    # configuration of the logging in the log file
    file_handler = RotatingFileHandler('logs/using_api_examples.log', 'w', 10000000, 1)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # configuration of the logging in the terminal 
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    
    return logger


address_search = "http://localhost:5000/search"
address_group = "http://localhost:5000/group"
logger = config_logging()

try:
    #search ID list of all documents
    res_exemple1 = get(address_search).json()
    logger.info("ID list of all documents : ")
    logger.info(pformat(res_exemple1))

    # search ID list of documents that have ingesionDate equal to 2020/03/04
    ingestionDate_eq = "2020/03/04"
    res_exemple2 = get(address_search, data={"ingestionDate_eq":ingestionDate_eq}).json()
    logger.info("ID list of documents that have ingesionDate equal to %s : " % ingestionDate_eq)
    logger.info(pformat(res_exemple2))

    # search ID list of documents that have ingesionDate between to 2020/03/03 and 2020/03/05
    ingestionDate_gt="2020/03/03"
    ingestionDate_lt="2020/03/05"
    res_exemple3 = get(address_search, data={"ingestionDate_gt":ingestionDate_gt, "ingestionDate_lt":ingestionDate_lt}).json()
    logger.info("ID list of documents that have ingesionDate between to %s and %s : " % (ingestionDate_gt, ingestionDate_lt))
    logger.info(pformat(res_exemple3))

    # search ID list of documents that have sourceName "La Voix du Nord"
    sourceName="La Voix du Nord"
    res_exemple4 = get(address_search, data={"sourceName":sourceName}).json()
    logger.info("ID list of documents that have sourceName \"%s\" : " % sourceName)
    logger.info(pformat(res_exemple4))

    # search ID list of documents that have topics "Éolien" or "Nucléaire"
    topics = ["Éolien", "Nucléaire"]
    res_exemple5 = get(address_search, data={"topics":topics}).json()
    logger.info("ID list of documents that have one of topics in %s : " % topics )
    logger.info(pformat(res_exemple5))

    # search ID list of documents that have field "namedEntities"
    res_exemple6 = get(address_search, data={"namedEntities":True}).json()
    logger.info("ID list of documents that have field \"namedEntities\": ")
    logger.info(pformat(res_exemple6))

    # search ID list of documents that have locations "Paris" or "Lille"
    locations = ["Paris", "Lille"]
    res_exemple7 = get(address_search, data={"locations":locations}).json()
    logger.info("ID list of documents that have one of locations in %s : " % locations)
    logger.info(pformat(res_exemple7))

    # Retrieve the number of articles considered “non spam” (isSpamProb <0.1) published per day
    res_exemple8 = get(address_group).json()
    logger.info("the number of articles considered “non spam” (isSpamProb <0.1) published per day : ")
    logger.info(pformat(res_exemple8))

    # Retrieve the number of articles considered “non spam” (isSpamProb <0.2) published per day
    isSpamProb = 0.2
    res_exemple9 = get(address_group, data={"isSpamProb":isSpamProb}).json()
    logger.info("the number of articles considered “non spam” (isSpamProb <%s) published per day : " % isSpamProb)
    logger.info(pformat(res_exemple9))

except ConnectionError :
    logger.error ("Connection Error : please launch the script python elastic_api_rest.py")
