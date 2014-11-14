import sys, base64, os, socket, signal
import urllib2, urllib, httplib, time, copy
import threading
import logging
import psycopg2, psycopg2.pool
import asyncore
import pyinotify
from Queue import LifoQueue
from threading import Condition, Thread
from collections import deque
from BaseHTTPServer import HTTPServer
from BaseHTTPServer import BaseHTTPRequestHandler
import urlparse, json
import logging, logging.handlers


USE_SENDFILE = False
try:
   from sendfile import sendfile
   USE_SENDFILE = True
except ImportError as e:
   pass


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
   
   def hasTransfered(self, handler, filename):
      
      basefile = os.path.basename(filename)
      
      cursor = None
      con = None

      try:
         con = self.dbp.getconn()
         
         cursor = con.cursor()
         # check if observation exists; if not then except; else add the entry
         cursor.execute("select exists(select 1 from ngas_files where file_id = %s)", [basefile])
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
      
      self.obsMap = {}
      self.obsMapLock = threading.RLock()
   
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
   
   
   def getFileStatus(self):
      # return number of files and type so we can tell the user whats left
      with self.obsMapLock:
         return 8, copy.deepcopy(self.obsMap)
   
   def fileAdded(self, path):
      try:
         obs, time, chan = self.splitFile(path)
         with self.obsMapLock:
            value = self.obsMap.get(obs)
            if value:
               value += 1
               self.obsMap[obs] = value
            else:
               self.obsMap[obs] = 1
      except:
         pass
   
   def fileComplete(self, path):
      try:
         obs, time, vcs, part = self.splitFile(path)
         with self.obsMapLock:
            value = self.obsMap.get(obs)
            if value:
               value = value - 1
               self.obsMap[obs] = value
      except:
         pass
   
   def hasTransfered(self, filename):    
      return self.db.hasTransfered(self, filename)

   
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
      

class MWADatabaseHandler(object):
   
   def __init__(self, dbhost, dbuser, dbname, dbpass, dbport):
   
      self.dbp = psycopg2.pool.ThreadedConnectionPool(minconn=2, \
                                                maxconn=12, \
                                                host=dbhost, \
                                                user=dbuser, \
                                                database=dbname, \
                                                password=dbpass, \
                                                port=dbport)
   
   def hasTransfered(self, handler, filename):
      
      basefile = os.path.basename(filename)
      
      cursor = None
      con = None

      try:
         con = self.dbp.getconn()
         
         cursor = con.cursor()
         # check if observation exists; if not then except; else add the entry
         cursor.execute("select exists(select 1 from data_files where filename = %s)", [basefile])
         row = cursor.fetchone()
         return row[0]
         
      except Exception as e:
         raise e

      finally:
         if cursor:
            cursor.close()
            
         if con:
            self.dbp.putconn(conn=con)


   def postTransferFileSuccess(self, handler, filename):
      
      obsid, time, vcs, lane = handler.splitFile(filename)
      size = os.stat(filename).st_size
      basefile = os.path.basename(filename)
      
      con = None
      cursor = None
       
      try:
         con = self.dbp.getconn()
         con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
   
         cursor = con.cursor()
         cursor.execute("INSERT INTO data_files (observation_num, filetype, size, filename, site_path, host, remote_archived) VALUES (%s, %s, %s, %s, %s, %s, True)",
                        [str(obsid), str(11), str(size), basefile, 'http://mwangas/RETRIEVE?file_id=' + basefile, vcs])

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
      
      self.obsMap = {}
      self.obsMapLock = threading.RLock()
   
   
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
      
   def getFileStatus(self):
      # return number of files and type so we can tell the user whats left
      with self.obsMapLock:
         return 11, copy.deepcopy(self.obsMap)
   
   def fileAdded(self, path):
      try:
         obs, time, vcs, part = self.splitFile(path)
         with self.obsMapLock:
            value = self.obsMap.get(obs)
            if value:
               value += 1
               self.obsMap[obs] = value
            else:
               self.obsMap[obs] = 1
      except:
         pass
   
   def fileComplete(self, path):
      try:
         obs, time, vcs, part = self.splitFile(path)
         with self.obsMapLock:
            value = self.obsMap.get(obs)
            if value:
               value = value - 1
               self.obsMap[obs] = value
      except:
         pass
   
   def hasTransfered(self, filename):
      return self.db.hasTransfered(self, filename)

   
   def transferFile(self, filename):
      code, resp, data = self.conn.transferFile(filename)
      if code != 200:
         raise Exception(data)
   
   
   def preTransferFile(self, filename):
      pass
   
   
   def postTransferFileSuccess(self, filename):
      self.db.postTransferFileSuccess(self, filename)
      #obsid, time, vcs, lane = self.splitFile(filename)
      # insert the successfully transfered file into the data_files table in the M&C database
      #self.db.insertVoltageFile(obsid, os.path.basename(filename), os.stat(filename).st_size, vcs)
      
      #do we want to delete the file?
      
   def postTransferFileError(self, filename):
      pass
   
   
   def matchFile(self, filename):
      try:
         self.splitFile(filename)
         return True
      except:
         return False


class Dequeue(object):
   
   def __init__(self):
      self.QUEUE = deque()
      self.cv = Condition(threading.RLock())
   
   def popleftnowait(self):
      return self.QUEUE.popleft()
   
   def popleft(self):
      with self.cv:
         while not bool(self.QUEUE):
            self.cv.wait(1)
            
         return self.QUEUE.popleft()

   def append(self, item):
      with self.cv:
         self.QUEUE.append(item)
         self.cv.notify()
   
   def appendUnique(self, item):
      unique = False
      
      with self.cv:
         try:
            #Removed the first occurrence of value. If not found, raises a ValueError.
            self.QUEUE.remove(item)
         except:
            unique = True
         
         self.QUEUE.append(item)
         self.cv.notify()
         
         return unique
   
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
            
            typeFileMap = {}
            for h in self.server.context.handlers:
               type, map = h.getFileStatus()
               typeFileMap[type] = map
            
            transmitList = self.server.context.getTransmitList()
            
            data = { 'status':statustext, 'queue':len(self.server.context.q), 'resend_queue':len(self.server.context.resendq), 'transmit': len(transmitList), 'files':typeFileMap }
            
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
            
         elif parsed_path.path.lower() == '/rescan'.lower():
            self.server.context.rescan()
            self.send_response(200)
            self.end_headers()
            
      except Exception as e:
         self.send_response(400)
         self.end_headers()


class Archiver(object):
   
   def __init__(self, dirs, watchdirs, handlers, concurrent=2, resend_wait=600):
      self.filelogger = logging.getLogger('archive-success')
      self.logger = logging.getLogger('archive')
      
      self.q = Dequeue()
      self.resendq = Dequeue()
      self.handlers = handlers
      self.sem = threading.Semaphore(concurrent)
      self.resend_wait = resend_wait
      
      self.transmitLock = threading.RLock()
      self.transmit = []
      
      # pause 
      self.pausecond = Condition(threading.RLock())
      self.pausebool = False

      if watchdirs is not None:
         if type(watchdirs) is not list:
            raise Exception('watchdirs not a list')
            
      if dirs is not None:
         if type(dirs) is not list:
            raise Exception('dirs not a list')
      
      self.dirs = dirs
      self.watchdirs = watchdirs
      
      self.logger.info('walking dirs: %s' % (dirs))
      self.logger.info('watching dirs: %s' % (watchdirs))
      
      self.resend = threading.Thread(name='_resendLoop', target=self._resendLoop, args=())
      self.resend.setDaemon(True)
      self.resend.start()
      
      class FilesystemCallback(pyinotify.ProcessEvent):
         def __init__(self, ad):
            self.ad = ad
         
         def process_IN_CLOSE_WRITE(self, event):
            if event.dir is False:
               handler = self._findHandler(event.pathname)
               if handler:
                  self.ad.logger.info('new file added to filesystem; file: %s' % (event.pathname))
                  if self.ad.q.appendUnique(event.pathname):
                     handler.fileAdded(event.pathname)
      
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
   
   def getTransmitList(self):
      with self.transmitLock:
         return copy.deepcopy(self.transmit)
     
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
      added = 0
      
      for folder, subs, files in os.walk(dir):
         for f in sorted(files):
            path = folder + '/' + f
            # only add files for which there is a handler
            handler = self._findHandler(path)
            if handler:   
               if self.q.appendUnique(path):
                  handler.fileAdded(path)
               added += 1
               
      return added
   
   
   def _findHandler(self, file):
      for h in self.handlers:
         if h.matchFile(file):
            return h
         
      return None
   
   
   def _worker(self, handler, file):
      
      try:
         with self.transmitLock:
            self.transmit.append(os.path.basename(file))
          
         try:
            # if we have already transfered this file then just ignore
            if handler.hasTransfered(file) is False:
               handler.preTransferFile(file)
               
               self.logger.info('transferring file; file: %s' % (file))
            
               handler.transferFile(file)
               
               self.logger.info('transferFile success; file: %s' % (file))
               
               try:
                  handler.postTransferFileSuccess(file)
                  self.logger.info('postTransferFileSuccess success; file: %s' % (file))
                  self.filelogger.info(file)
                  
                  handler.fileComplete(file)
                  
               except Exception as ex:
                  self.logger.error('postTransferFileSuccess error; file: %s error: %s' % (file, str(ex)))
            else:
               self.logger.info('file has already been transferred, ignoring; file: %s' % (file))
               
               handler.fileComplete(file)
               
         except Exception as e:
            handler.postTransferFileError(file)
            
            self.logger.error('transferFile error; putting on resend queue; file: %s error: %s' % (file, str(e)))
               
            # there was an error so put it on the resend queue
            self.resendq.append(file)
         
      
      finally:
         try:
            with self.transmitLock:
               self.transmit.remove(os.path.basename(file))
         except:
            pass
         
         self.sem.release()
   
   
   def _transferFile(self, handler, file):      
      t = threading.Thread(target=self._worker, args=(handler, file))
      t.start()


   def _processQueue(self):
      
      while True:
         # pause the thread
         with self.pausecond:
            while self.pausebool is True:
         	   self.pausecond.wait(1)
         
         self.sem.acquire()
         
         file = self.q.popleft()

         if file is None:
            break;
         
         handler = self._findHandler(file)
         if handler:   
            self._transferFile(handler, file)
         else:
            self.sem.release()
            self.logger.info('file does not match any handler; file: %s' % (file,))
            continue

   
   def pause(self):
      with self.pausecond:
         self.pausebool = True
         
      self.logger.info('pause called')
   
   def rescan(self):
      added = 0
      self.logger.info('rescan called')
      for d in self.dirs:
         added += self._walkPath(d)
         
      self.logger.info('rescan complete; unique files added: %s' % str(added))
   
   def resume(self):
      with self.pausecond:
         if self.pausebool is True:
            self.pausebool = False
            self.pausecond.notify()
            
      self.logger.info('resume called')
	
	
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
      self.logger.info('interrupted, shutting down...')
      
      # close command server
      if self.cmdserver:
         self.cmdserver.server_close()
         self.cmdthread.join()
      
      # put sentinal value of queue
      self.q.appendleft(None)
      
      # resume the queue if we are pause
      self.resume()