#!/usr/bin/python
import sys, base64, os, socket, signal
import urllib2, urllib, httplib, time
import threading
import logging
import psycopg2, psycopg2.pool
import asyncore
import pyinotify
from Queue import LifoQueue
from threading import Condition, Thread
from collections import deque
from ConfigParser import SafeConfigParser
from BaseHTTPServer import HTTPServer
from BaseHTTPServer import BaseHTTPRequestHandler
import urlparse, json


USE_SENDFILE = False
try:
   from sendfile import sendfile
   USE_SENDFILE = True
except ImportError as e:
   pass


import logging, logging.handlers

APP_PATH = os.path.dirname(os.path.realpath(__file__))
path = APP_PATH + '/log/'

if not os.path.exists(path):
   os.makedirs(path)

logger = logging.getLogger('archive')
logger.setLevel(logging.DEBUG)
logger.propagate = False
rot = logging.handlers.RotatingFileHandler(path + 'archive.log', maxBytes=33554432)
rot.setLevel(logging.DEBUG)
rot.setFormatter(logging.Formatter('%(asctime)s, %(levelname)s, %(message)s'))
logger.addHandler(rot)

filelogger = logging.getLogger('archive-success')
filelogger.setLevel(logging.DEBUG)
filelogger.propagate = False
filerot = logging.handlers.RotatingFileHandler(path + 'archive-file-success.log', maxBytes=33554432)
filerot.setLevel(logging.DEBUG)
filerot.setFormatter(logging.Formatter('%(asctime)s, %(levelname)s, %(message)s'))
filelogger.addHandler(filerot)


class NGASHttpPushConnector(object):
   
   def __init__(self, url, command, username, password, mimetype):
      self.url = url
      self.command = command
      self.username = username
      self.password = password
      self.mimetype = mimetype


   def transferFile(self, fullpath):

      filename = os.path.basename(fullpath)
      if not filename:
         raise Exception('could not extract basename from %s' % fullpath)
      
      filesize = os.stat(fullpath).st_size   
   
      file = None
      conn = None
      
      try:
         conn = httplib.HTTPConnection(self.url)
         
         conn.putrequest("POST", self.command)
         
         base64string = base64.encodestring('%s:%s' % (self.username, self.password)).replace('\n', '')
         conn.putheader("Authorization", "Basic %s" % base64string)
         conn.putheader("Content-disposition", "attachment; filename=%s" % filename)
         conn.putheader("Content-length", filesize)
         conn.putheader("Host", socket.gethostname())
         conn.putheader("Content-type", self.mimetype)
         conn.endheaders()
         
         blocksize = 65536
   
         file = open(fullpath, "rb")
         
         # use zero copy kernel copy or do a user space copy (more expensive)
         if USE_SENDFILE:
            offset = 0
            while True:
               sent = sendfile(conn.sock.fileno(), file.fileno(), offset, blocksize)
               if sent == 0:
                  break  # EOF
               offset += sent
         else:
            sent = 0
            while True:
               # read to EOF
               databuff = file.read(blocksize)
               if databuff:
                  # send all data out
                  conn.sock.sendall(databuff)
                  sent += len(databuff)
               else:
                  break
         
            if sent != filesize:
               raise Exception("data sent does not match filesize: %s %s" % (str(sent), str(filesize)))
            
         # read the response
         resp = conn.getresponse()
         data = ''
         while True:
            buff = resp.read()
            if buff:
               data += buff
            else:
               break
            
         return resp.status, resp.reason, data
      
      finally:
         if file:
            file.close()
            
         if conn:
            conn.close()


class MWADatabaseHandler(object):
   
   def __init__(self, dbhost, dbuser, dbname, dbpass, dbport):
   
      self.dbp = psycopg2.pool.ThreadedConnectionPool(minconn=2, \
                                                maxconn=12, \
                                                host=dbhost, \
                                                user=dbuser, \
                                                database=dbname, \
                                                password=dbpass, \
                                                port=dbport)
   
   def hasVoltageFileTransfered(self, file):
      
      cursor = None
      con = None

      try:
         con = self.dbp.getconn()
         
         cursor = con.cursor()
         # check if observation exists; if not then except; else add the entry
         cursor.execute("select exists(select 1 from data_files where filename = %s)", [file])
         row = cursor.fetchone()
         return row[0]
         
      except Exception as e:
         raise e

      finally:
         if cursor:
            cursor.close()
            
         if con:
            self.dbp.putconn(conn=con)


   def insertVoltageFile(self, obsid, filename, size, host):
      con = None
      cursor = None
       
      try:
         con = self.dbp.getconn()
         con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
   
         cursor = con.cursor()
         cursor.execute("INSERT INTO data_files (observation_num, filetype, size, filename, site_path, host, remote_archived) VALUES (%s, %s, %s, %s, %s, %s, True)",
                        [str(obsid), str(11), str(size), filename, 'http://mwangas/RETRIEVE?file_id=' + filename, host])

      except Exception, e:
         if con:
            con.rollback()
         raise e
      else:
         if con:
            con.commit()
      finally:
         if cursor:
           cursor.close()
   
         if con:
           self.dbp.putconn(conn=con)
      

   
class MWAVoltageDataFileHandler(object):
   
   def __init__(self, connector, db):
      self.conn = connector
      self.db = db
   
   
   def splitFile(self, filename):
      
      try:
         file = os.path.basename(filename)
         if '.dat' not in file:
            raise Exception('dat extension not found')
      
         part = file.split('_')
         if 'vcs' not in part[2]:
            raise Exception('vcs not found in 3rd part')
         
         return (int(part[0]), int(part[1]), part[2], int(part[3].split('.')[0]))
               
      except Exception as e:
         raise Exception('invalid voltage data filename %s' % file)
   
   
   def hasTransfered(self, filename):    
      return self.db.hasVoltageFileTransfered(os.path.basename(filename))

   
   def transferFile(self, filename):
      code, resp, data = self.conn.transferFile(filename)
      if code != 200:
         raise Exception(data)
   
   
   def preTransferFile(self, filename):
      pass
   
   
   def postTransferFileSuccess(self, filename):
      
      obsid, time, vcs, lane = self.splitFile(filename)
      # insert the successfully transfered file into the data_files table in the M&C database
      self.db.insertVoltageFile(obsid, os.path.basename(filename), os.stat(filename).st_size, vcs)
      
      #do we want to delete the file?
      
   def postTransferFileError(self, filename):
      pass
   
   
   def matchFile(self, filename):
      try:
         self.splitFile(filename)
         return True
      except:
         return False



class MWAVoltageDataVariantFileHandler(MWAVoltageDataFileHandler):
   
   def splitFile(self, filename):
      
      try:
         file = os.path.basename(filename)
         if '.dat' not in file:
            raise Exception('dat extension not found')
      
         part = file.split('_')
         if 'ch' not in part[2]:
            raise Exception('ch not found in 3rd part')
         
         obsid = int(part[0])
         chan = part[2]
         tm = int(part[3].split('.')[0])
         lane = 0
         
         return obsid, tm, chan, lane
               
      except Exception as e:
         raise Exception('invalid voltage data variant filename %s' % file)
   


class ActivePool(object):
   def __init__(self):
      self.active = []
      self.lock = threading.Lock()
   
   def makeActive(self, thd):
      with self.lock:
         self.active.append(thd)
   
   def makeInactive(self, thd):
      with self.lock:
         self.active.remove(thd)


class Dequeue(object):
   
   def __init__(self):
      self.QUEUE = deque()
      self.cv = Condition(threading.RLock())
   
   def popleftnowait(self):
      return self.QUEUE.popleft()
   
   def popleft(self):
      with self.cv:
         while not bool(self.QUEUE):
            self.cv.wait(0.01)
            
         return self.QUEUE.popleft()

   def append(self, item):
      with self.cv:
         self.QUEUE.append(item)
         self.cv.notify()
      
   def appendleft(self, item):
      with self.cv:
         self.QUEUE.appendleft(item) 
         self.cv.notify()
   
   def __len__(self):
   	return len(self.QUEUE) 


class MyHTTPServer(HTTPServer):
   def __init__(self, *args, **kw):
      HTTPServer.__init__(self, *args, **kw)
      self.context = None


class HTTPGetHandler(BaseHTTPRequestHandler):

   def do_GET(self):
      
      parsed_path = urlparse.urlparse(self.path)
      
      try:
         if parsed_path.path.lower() == '/status'.lower():
            
            statustext = 'running'
            if self.server.context.pausebool:
                statustext = 'paused'
            
            data = { 'status':statustext, 'queue':len(self.server.context.q) }
            
            self.send_response(200)
            self.end_headers()
            self.wfile.write(json.dumps(data))
         
         elif parsed_path.path.lower() == '/pause'.lower():
            self.server.context.pause()
            self.send_response(200)
            self.end_headers()
           
         elif parsed_path.path.lower() == '/resume'.lower():
            self.server.context.resume()
            self.send_response(200)
            self.end_headers()
             
      except Exception as e:
         self.send_response(400)
         self.end_headers()


class ArchiveDaemon(object):
   
   def __init__(self, dirs, watchdirs, handlers, concurrent=1, resend_wait=20):
      self.q = Dequeue()
      self.resendq = Dequeue()
      self.pool = ActivePool()
      self.handlers = handlers
      self.sem = threading.Semaphore(concurrent)
      self.resend_wait = resend_wait
      
      # pause 
      self.pausecond = Condition(threading.RLock())
      self.pausebool = False

      if watchdirs is not None:
         if type(watchdirs) is not list:
            raise Exception('watchdirs not a list')
            
      if dirs is not None:
         if type(dirs) is not list:
            raise Exception('dirs not a list')
      
      logger.info('walking dirs: %s' % (dirs))
      logger.info('watching dirs: %s' % (watchdirs))
      
      self.resend = threading.Thread(name='_resendLoop', target=self._resendLoop, args=())
      self.resend.setDaemon(True)
      self.resend.start()
      
      class FilesystemCallback(pyinotify.ProcessEvent):
         def __init__(self, ad):
            self.ad = ad
         
         def process_IN_CLOSE_WRITE(self, event):
            if event.dir is False:
               logger.info('new file added to filesystem; file: %s' % (event.pathname))
               self.ad.q.append(event.pathname)
      
      # walk all the specified directories and add all the files   
      for d in dirs:
         if d:
            self._walkPath(d)
      
      # create thread to get callbacks from filesystem i.e. creates
      self.fswatch = threading.Thread(name='_filesystemLoop', target=self._filesystemLoop, args=(watchdirs, FilesystemCallback(self)))
      self.fswatch.setDaemon(True)
      self.fswatch.start()

      self.cmdserver = MyHTTPServer(('', 7900), HTTPGetHandler)
      self.cmdserver.context = self

      self.cmdthread = threading.Thread(name='_commandLoop', target=self._commandLoop, args=(self.cmdserver,))
      self.cmdthread.setDaemon(True)
      self.cmdthread.start()

      signal.signal(signal.SIGINT, self._signalINT)
      signal.signal(signal.SIGTERM, self._signalINT)
      
     
   def _commandLoop(self, cmdserver):
      try:
         # start server
         cmdserver.serve_forever()
      except Exception as e:
         pass


   def _resendLoop(self):
      while True:      	
         # don't resend for resend_wait seconds
         time.sleep(self.resend_wait)
         
         # take off all files off the resend queue and add it to the send queue
         while True:
            try:
               element = self.resendq.popleftnowait()
               self.q.append(element)
            except IndexError:
               # no more elements in resend queue
               break
   

   def _filesystemLoop(self, dirs, callback):
      wm = pyinotify.WatchManager()
      mask = pyinotify.IN_CLOSE_WRITE           
      notifier = pyinotify.AsyncNotifier(wm, callback)
      
      for d in dirs:
         if d:
            wdd = wm.add_watch(d, mask, rec=True, auto_add=True)
         
      asyncore.loop()


   def _signalINT(self, signal, frame):
      self.stop()
   
   
   def _walkPath(self, dir):
      for folder, subs, files in os.walk(dir):
         for f in sorted(files):
            self.q.append(folder + '/' + f)
   
   
   def _findHandler(self, file):
      for h in self.handlers:
         if h.matchFile(file):
            return h
         
      return None
   
   
   def _worker(self, handler, file):
      
      thd = threading.currentThread()
      try:   
         self.pool.makeActive(thd)
         
         try:
            # if we have already transfered this file then just ignore
            if handler.hasTransfered(file) is False:
               handler.preTransferFile(file)
               
               logger.info('transferring file; file: %s' % (file))
               
               handler.transferFile(file)
               
               logger.info('transferFile success; file: %s' % (file))
               
               try:
                  handler.postTransferFileSuccess(file)
                  logger.info('postTransferFileSuccess success; file: %s' % (file))
                  filelogger.info(file)
                  
               except Exception as ex:
                  logger.error('postTransferFileSuccess error; file: %s error: %s' % (file, str(ex)))
            else:
               logger.info('file has already been transferred, ignoring; file: %s' % (file))
               
         except Exception as e:
            handler.postTransferFileError(file)
            
            logger.error('transferFile error; putting on resend queue; file: %s error: %s' % (file, str(e)))
               
            # there was an error so put it on the resend queue
            self.resendq.append(file)
         
      
      finally:
         self.pool.makeInactive(thd)
         self.sem.release()
   
   
   def _transferFile(self, handler, file):
      self.sem.acquire()      
      t = threading.Thread(target=self._worker, args=(handler, file))
      t.start()


   def _processQueue(self):
      
      while True:
         # pause the thread
         with self.pausecond:
            while self.pausebool is True:
         	   self.pausecond.wait(0.01)
               
         file = self.q.popleft()

         if file is None:
            break;
         
         handler = self._findHandler(file)
         if handler:
            self._transferFile(handler, file)
         else:
            continue
   
   
   def pause(self):
      with self.pausecond:
         self.pausebool = True
      logger.info('pause called')
   
   def resume(self):
      with self.pausecond:
         if self.pausebool is True:
            self.pausebool = False
      logger.info('resume called')
	
	
   def start(self):
      self._processQueue()
      
      while True:
         if len(threading.enumerate()) <= 4:
            break;
                     
         for t in threading.enumerate():
            if t.name in ['_commandLoop', '_filesystemLoop', '_resendLoop', 'MainThread']:
               continue
              
            t.join(0.5)
            if t.isAlive():
                continue

      
   def stop(self):
      logger.info('interrupted, shutting down...')
      
      # close command server
      if self.cmdserver:
         self.cmdserver.server_close()
         self.cmdthread.join()
      
      # put sentinal value of queue
      self.q.appendleft(None)
      
      # resume the queue if we are pause
      self.resume()
      

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
   h1 = MWAVoltageDataVariantFileHandler(ngas, db)
   h2 = MWAVoltageDataFileHandler(ngas, db)
   
   dirs = config.get("Archiver", "dirs").split(',')
   watchdirs = config.get("Archiver", "watchdirs").split(',')
   concurrent = config.getint("Archiver", "concurrent")

   a = ArchiveDaemon(dirs, watchdirs, [h1, h2], concurrent=concurrent)
   a.start()
   
   logger.info('stopped')

if __name__ == "__main__":
   try:
      main()
      sys.exit(0)
   except Exception as e:
      import traceback
      traceback.print_exc()
      logger.error(str(e))
      sys.exit(1)
      