#!/usr/bin/python
from archiver import *
import os, sys, base64
import logging, logging.handlers
from ConfigParser import SafeConfigParser

APP_PATH = os.path.dirname(os.path.realpath(__file__))
path = APP_PATH + '/log/'

if not os.path.exists(path):
   os.makedirs(path)

logger = logging.getLogger('archive')
logger.setLevel(logging.DEBUG)
logger.propagate = False
rot = logging.FileHandler(path + 'archive.log')
rot.setLevel(logging.DEBUG)
rot.setFormatter(logging.Formatter('%(asctime)s, %(levelname)s, %(message)s'))
logger.addHandler(rot)

filelogger = logging.getLogger('archive-success')
filelogger.setLevel(logging.DEBUG)
filelogger.propagate = False
filerot = logging.FileHandler(path + 'archive-file-success.log')
filerot.setLevel(logging.DEBUG)
filerot.setFormatter(logging.Formatter('%(asctime)s, %(levelname)s, %(message)s'))
filelogger.addHandler(filerot)


def main():
   
   logger.info('starting archiver...')
   
   # load up config file
   config = SafeConfigParser()
   config.readfp(open(APP_PATH + '/' + 'archive.cfg', "r"))
   
   dbhost = config.get("Database", "dbhost")
   dbname = config.get("Database", "dbname")
   dbuser = config.get("Database", "dbuser")
   dbpass = base64.b64decode(config.get("Database", "dbpass"))
   dbport = config.get("Database", "dbport")
   
   purl = config.get("Pawsey", "url")
   pcmd = config.get("Pawsey", "cmd")
   pmime = config.get("Pawsey", "mime")
   puser = config.get("Pawsey", "user")
   ppass = base64.b64decode(config.get("Pawsey", "pass"))
                
   db = MWADatabaseHandler(dbhost, dbname, dbuser, dbpass, dbport)
   ngas = NGASHttpPushConnector(purl, pcmd, puser, ppass, pmime)
   h1 = MWAVoltageDataFileHandler(ngas, db)
   
   dirs = config.get("Archiver", "dirs").split(',')
   watchdirs = config.get("Archiver", "watchdirs").split(',')
   concurrent = config.getint("Archiver", "concurrent")

   a = Archiver(dirs, watchdirs, [h1], concurrent=concurrent)
   a.start()
   
   logger.info('stopped')

if __name__ == "__main__":
   try:
      main()
      sys.exit(0)
   except Exception as e:
      logger.error(str(e))
      sys.exit(1)