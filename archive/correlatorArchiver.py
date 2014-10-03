#!/usr/bin/python
from archiver import *
import os, sys, base64, urllib
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



class NGASMoveConnector(object):
   
   def __init__(self, url, mimetype, username, password):
      self.url = url
      self.mimetype = mimetype
      self.username = username
      self.password = password


   def transferFile(self, fullpath):

      logger.info('Moving file now %s' % (fullpath,))
      
      try:
         base64string = base64.encodestring('%s:%s' % (self.username, self.password)).replace('\n', '')
         headers = {"Authorization": "Basic %s" % base64string}
      
         req = 'LARCHIVE?fileUri=%s&mimeType=%s' % (fullpath, self.mimetype)
         conn = httplib.HTTPConnection(self.url)
         conn.request('GET', req, None, headers)
      
         # read the response
         resp = conn.getresponse()
         data = ''
         while True:
            buff = resp.read()
            if buff:
               data += buff
            else:
               break
      
         print resp.status, resp.reason, data
      
      finally:
         if conn:
            conn.close()



class NGASDatabaseHandler(object):
   
   def __init__(self, dbhost, dbuser, dbname, dbpass, dbport):
   
      self.dbp = psycopg2.pool.ThreadedConnectionPool(minconn=2, \
                                                maxconn=12, \
                                                host=dbhost, \
                                                user=dbuser, \
                                                database=dbname, \
                                                password=dbpass, \
                                                port=dbport)
   
   def hasFileBeenTransfered(self, file):
      
      cursor = None
      con = None

      try:
         con = self.dbp.getconn()
         
         cursor = con.cursor()
         # check if observation exists; if not then except; else add the entry
         cursor.execute("select exists(select 1 from ngas_files where file_id = %s)", [file])
         row = cursor.fetchone()
         return row[0]
         
      except Exception as e:
         raise e

      finally:
         if cursor:
            cursor.close()
            
         if con:
            self.dbp.putconn(conn=con)


class MWACorrelatorFitsDataFileHandler(object):
   
   def __init__(self, connector, db):
      self.conn = connector
      self.db = db
   
   
   def splitFile(self, filename):
      
      try:
         #1096202392_20141001123939_gpubox13_00.fits
         file = os.path.basename(filename)
         if '.fits' not in file:
            raise Exception('fits extension not found')
      
         part = file.split('_')
         if 'gpubox' not in part[2]:
            raise Exception('gpubox not found in 3rd part')
         
         return (int(part[0]), int(part[1]), part[2])
               
      except Exception as e:
         raise Exception('invalid correlator data filename %s' % file)
   
   
   def hasTransfered(self, filename):    
      return self.db.hasFileBeenTransfered(os.path.basename(filename))

   
   def transferFile(self, filename):
      code, resp, data = self.conn.transferFile(filename)
      if code != 200:
         raise Exception(data)
   
   
   def preTransferFile(self, filename):
      pass
   
   
   def postTransferFileSuccess(self, filename):
      pass 

      
   def postTransferFileError(self, filename):
      pass
   
   
   def matchFile(self, filename):
      try:
         self.splitFile(filename)
         return True
      except:
         return False


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
   pmime = config.get("Pawsey", "mime")
   puser = config.get("Pawsey", "user")
   ppass = config.get("Pawsey", "pass")
                
   db = NGASDatabaseHandler(dbhost, dbname, dbuser, dbpass, dbport)
   ngas = NGASMoveConnector(purl, pmime, puser, ppass)
   h1 = MWACorrelatorFitsDataFileHandler(ngas, db)
   
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