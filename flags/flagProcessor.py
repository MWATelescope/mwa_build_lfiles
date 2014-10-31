import os, sys, base64, socket, struct
import logging, logging.handlers
import threading, time, signal, subprocess
from ConfigParser import SafeConfigParser
from threading import Condition, Thread
import psycopg2, psycopg2.pool
from collections import deque
from BaseHTTPServer import HTTPServer
from BaseHTTPServer import BaseHTTPRequestHandler
import urlparse, json

APP_PATH = os.path.dirname(os.path.realpath(__file__))
path = APP_PATH + '/log/'

if not os.path.exists(path):
   os.makedirs(path)

logger = logging.getLogger('flagger')
logger.setLevel(logging.DEBUG)
logger.propagate = False
rot = logging.FileHandler(path + 'flagger.log')
rot.setLevel(logging.DEBUG)
rot.setFormatter(logging.Formatter('%(asctime)s, %(levelname)s, %(message)s'))
logger.addHandler(rot)


class Dequeue(object):
   
   def __init__(self):
      self.QUEUE = deque()
      self.cv = Condition(threading.Lock())
   
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
      
   def appendleft(self, item):
      with self.cv:
         self.QUEUE.appendleft(item)
         self.cv.notify()
   
   def remove(self, item):
      with self.cv:
         self.QUEUE.remove(item)
   
   def __len__(self):
      return len(self.QUEUE)


class DummyHandler(object):
   
   def processFlags(self, obsid, numfiles):
      logger.info('Processing flags: %s' % (str(obsid)))
      time.sleep(0.05)


class FornaxFlagHandler(object):
  
   def __init__(self):
      self.fp = None

      config = SafeConfigParser()
      config.readfp(open(APP_PATH + '/' + 'flagger.cfg', "r"))
      
      self.OBS_DOWNLOAD = '/scratch/mwaops/flags/scripts/obsdownload.py'
      self.COTTER = '/group/mwaops/CODE/bin/cotter'
      self.SCRATCH = '/scratch/mwaops/flags/tmp'
      self.LOG = '/home/dpallot/mwaops/flags/log/cotter'
   
      self.WGET_HEADER = 'ssh -t fornax \'wget http://ngas01.ivec.org/metadata/fits?obs_id=%(OBSID)s -O %(SCRATCH)s/%(OBSID)s/%(OBSID)s.metafits\''
   
      self.COPYQ = 'ssh -t fornax \'qsub -I -q copyq -N obsdownload -l walltime=00:59:00 -l select=1:ncpus=1:mem=2gb  -W group_list=mwaops  -- %(CMD)s -o %(OBSID)s -d %(SCRATCH)s\''
   
      self.COTTERQ = 'ssh -t fornax \'qsub -I -V -q workq -N cotter -l walltime=00:59:00 -l select=1:ncpus=12:mem=64gb  -W group_list=mwaops  -- %(COTTER)s -allowmissing -absmem 64 %(IFLAG)s -m %(SCRATCH)s/%(OBSID)s/%(OBSID)s.metafits -o %(SCRATCH)s/%(OBSID)s/%(OBSID)s_%%%%.mwaf %(SCRATCH)s/%(OBSID)s/*.fits\''
      
      self.ZIP = 'ssh -t fornax \'/usr/bin/zip -r %(ZIPFILE)s %(FLAGS)s\''
      
      self.ARCHIVECLIENT = 'ssh -t fornax \'/scratch/mwaops/ngas/ngamsCClient -host fe1.pawsey.ivec.org -port 7777 -auth ' + config.get("Pawsey", "archiveauth") + ' -cmd QARCHIVE -mimeType application/octet-stream -fileUri %(FILE)s\''


   def tarUpFlagFiles(self, direc, obsid):
    
      name =  str(obsid) + '_flags.zip'

      cmd = self.ZIP % { 'ZIPFILE' : direc + name, 'FLAGS' : direc + '*.mwaf' }

      proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True, close_fds=True)

      out, err = proc.communicate()
      exitcode = proc.returncode

      if exitcode != 0:
         raise Exception(out)

      statcmd = 'ssh -t fornax \'stat --printf="%s" ' + direc + name + '\''

      proc = subprocess.Popen(statcmd, stdout=subprocess.PIPE, shell=True, close_fds=True)

      out, err = proc.communicate()
      exitcode = proc.returncode

      if exitcode != 0:
         raise Exception(out)

      return direc + name, name, int(out)


   def archiveFile(self, fullpath, size):

      cmd = self.ARCHIVECLIENT % { 'FILE' : fullpath }

      proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True, close_fds=True)

      out, err = proc.communicate()
      exitcode = proc.returncode

      if exitcode != 0:
        raise Exception(out)

      print out


   def hasFreqChanged(self, obsid):
   
      con = None
      cursor = None
   
      try:
         con = self.fp.dbp.getconn()
         cursor = con.cursor()
         cursor.execute("select frequencies from rf_stream where starttime <= %s order by starttime desc limit 2;", [str(obsid)])
         
         rows = cursor.fetchall()

         if len(rows) != 2:
            raise Exception('could not get frequencies for %s' % (str(obsid)))
         
         return (rows[0] != rows[1])
            
      finally:
         if cursor:
            cursor.close()
         
         if con:
            self.fp.dbp.putconn(conn=con)
      
   
   def updateMCDatabase(self, obsid, filename, size):
      con = None
      cursor = None

      try:
         con = self.fp.sitedbp.getconn()
         con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
         
         cursor = con.cursor()

         cursor.execute("SELECT * from data_files where observation_num = %s AND filename = %s", [str(obsid), str(filename)])
         row = cursor.fetchone()
         if row:
            cursor.execute("update data_files set size = %s where filename = %s and filetype = %s", [str(size), str(filename), str(10)])
         else:
            uri = 'http://mwangas/RETRIEVE?file_id=' + filename            
            cursor.execute("INSERT INTO data_files (observation_num, filetype, size, filename, site_path, remote_archived) VALUES (%s, %s, %s, %s, %s, TRUE)", [str(obsid), str(10), str(size), filename, uri])


      except Exception as e:
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
            self.fp.sitedbp.putconn(conn=con)
    

   def processFlags(self, obsid, numfiles):
      logger.info('Processing flags: %s' % (str(obsid)))

      logger.info('Downloading files: %s' % (str(obsid)))
      # get the files
      downloadcmd = self.COPYQ % { 'CMD' : self.OBS_DOWNLOAD, 'OBSID' : str(obsid), 'SCRATCH' : self.SCRATCH }

      p1 = subprocess.Popen(downloadcmd, shell=True, close_fds=True)
      output, errors = p1.communicate()
      
      if (p1.returncode != 0):
         raise Exception('error downloading observation %s' % (str(obsid)))
      
      logger.info('Downloading files success: %s' % (str(obsid)))

      logger.info('Downloading metafits file: %s' % (str(obsid)))

      wgetheadercmd = self.WGET_HEADER % {'OBSID' : str(obsid), 'SCRATCH' : self.SCRATCH} 
      
      p2 = subprocess.Popen(wgetheadercmd, shell=True)
      output, errors = p2.communicate()
      if (p2.returncode != 0):
         raise Exception('error getting fits header for observation %s' % (str(obsid)))
      
      flagstr = '-initflag 0'
      if self.hasFreqChanged(obsid):
         logger.info('Frequency has changed: %s' % (str(obsid)))
         flagstr = ''
      else:
         logger.info('Frequency has NOT changed: %s' % (str(obsid)))

      cottercmd = self.COTTERQ % {'COTTER' : self.COTTER, 'IFLAG': flagstr, 'SCRATCH' : self.SCRATCH, 'OBSID' : str(obsid)}

      logger.info('Cottering: %s' % (str(obsid)))

      p3 = subprocess.Popen(cottercmd, shell=True, stdout=subprocess.PIPE, close_fds=True)
      output, errors = p3.communicate()

      with open(self.LOG + '/' + str(obsid) + '_cotter.log', 'w') as f:
         f.write(output)
         f.flush()

      if (p3.returncode != 0):
         raise Exception('error cottering observation %s' % (str(obsid)))

      logger.info('Cottering success: %s' % (str(obsid)))

      logger.info('Taring: %s' % (str(obsid)))

      fullpath, filename, size = self.tarUpFlagFiles(self.SCRATCH + '/' + str(obsid) + '/', obsid)

      if size < 9000:
         raise Exception('Tar file is too small, there has been an a problem: %s; filename: %s; size: %s' (str(obsid), str(filename), str(size)))

      logger.info('Taring success: %s; filename: %s; size: %s' % (str(obsid), str(filename), str(size)))

      logger.info('Archiving: %s filename: %s' % (str(obsid), str(filename)))

      self.archiveFile(fullpath, size)

      logger.info('Archiving success: %s filename: %s' % (str(obsid), str(filename)))

      logger.info('Updating MC Database: %s filename: %s' % (str(obsid), str(filename)))

      self.updateMCDatabase(obsid, filename, size)

      logger.info('Updating MC Database success: %s filename: %s' % (str(obsid), str(filename)))
            
      logger.info('Removing processing directory: %s' % (str(obsid)))
      
      # remove files
      rmcmd = 'ssh -t fornax \'rm -rf ' + self.SCRATCH + '/' + str(obsid) + '/' + '\''
      proc = subprocess.Popen(rmcmd, stdout=subprocess.PIPE, shell=True, close_fds=True)
      out, err = proc.communicate()
      exitcode = proc.returncode

      if exitcode != 0:
         raise Exception(out)

      logger.info('Processing flags success: %s' % (str(obsid)))



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
            
            processing = []
            with self.server.context.plock:
               processing = list(self.server.context.processing)
             
            processingstr = ''
            for i in processing:
               processingstr += str(i) + ' '
               
            data = { 'status':statustext, 'queue':len(self.server.context.q), 'processing': processingstr }
            
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
         
         elif parsed_path.path.lower() == '/kill'.lower():
            self.server.context.stop()
            self.send_response(200)
            self.end_headers()
             
      except Exception as e:
         print e
         self.send_response(400)
         self.end_headers()



class FlagProcessor(object):
   
   def __init__(self, handler, concurrent=4, resend_wait=300):
      self.q = Dequeue()
      self.resendq = Dequeue()
      self.sem = threading.Semaphore(concurrent)
      self.resend_wait = resend_wait
      self.handler = handler
      self.handler.fp = self
      
      # keep track of what we are processing at the moment
      self.processing = []
      self.plock = Condition(threading.Lock())
      self.pcount= 0
      
      self.pausecond = Condition(threading.Lock())
      self.pausebool = False
      
      config = SafeConfigParser()
      config.readfp(open(APP_PATH + '/' + 'flagger.cfg', "r"))
      
      self.dbp = psycopg2.pool.ThreadedConnectionPool(minconn=2, \
                                          maxconn=8, \
                                          host=config.get("Database", "dbhost"), \
                                          user=config.get("Database", "dbuser"), \
                                          database=config.get("Database", "dbname"), \
                                          password=base64.b64decode(config.get("Database", "dbpass")), \
                                          port=5432)

      self.sitedbp = psycopg2.pool.ThreadedConnectionPool(minconn=2, \
                                          maxconn=8, \
                                          host=config.get("Database", "sitedbhost"), \
                                          user=config.get("Database", "sitedbuser"), \
                                          database=config.get("Database", "sitedbname"), \
                                          password=base64.b64decode(config.get("Database", "sitedbpass")), \
                                          port=5432)
      
      self.ngasdb = psycopg2.pool.ThreadedConnectionPool(minconn=2, \
                                           maxconn=8, \
                                           host=config.get("Database", "ndbhost"), \
                                           user=config.get("Database", "ndbuser"), \
                                           database=config.get("Database", "ndbname"), \
                                           password=config.get("Database", "ndbpass"), \
                                           port=5432)
      
      self.dmgetHost = config.get("Pawsey", "dmgethost")
      self.dmgetPort = config.getint("Pawsey", "dmgetport")


      logger.info('Getting observation set...')
      
      rowset = self._getObsProcessingList()
      if rowset:
         logger.info('Number of observations to flag: %s' % (len(rowset)))

         for r in rowset:
            self.q.append(r)


      self.processRecent = threading.Thread(name='_processRecent', target=self._processRecent, args=())
      self.processRecent.setDaemon(True)
      self.processRecent.start()
      

      self.cmdserver = MyHTTPServer(('', 7900), HTTPGetHandler)
      self.cmdserver.context = self

      self.cmdthread = threading.Thread(name='_commandLoop', target=self._commandLoop, args=(self.cmdserver,))
      self.cmdthread.setDaemon(True)
      self.cmdthread.start()
      
      
      signal.signal(signal.SIGINT, self._signalINT)
      signal.signal(signal.SIGTERM, self._signalINT)

   
   def _processRecent(self):
      
      while True:
         
         time.sleep(86400)
         
         with self.plock:
            if self.pcount > 0:
               logger.info('Still processing recent list, continuing.')
               continue

         try:
            obs = self._getRecentObsProcessingList()
            
            logger.info('Recent observation list: %s' % (str(len(obs))))
            
            with self.plock:
               
               for o in obs:
                  obsid = o[0]
                  
                  # if we are already processing this observation then ignore it
                  if obsid in self.processing:
                     self.pcount += 1
                     continue
                  
                  # remove from queue;
                  try:
                     self.q.remove(o)
                  except:
                     pass
                  
                  self.q.appendleft(o)
                  self.pcount += 1
            
         except Exception as e:
            logger.error('_processRecent: %s' % (str(e)))
            


   def _commandLoop(self, cmdserver):
      try:
         # start server
         cmdserver.serve_forever()
      except Exception as e:
         pass


   def _signalINT(self, signal, frame):
      self.stop()
      
   
   def _getRecentObsProcessingList(self):
      cursor = None
      con = None

      try:
         con = self.dbp.getconn()

         cursor = con.cursor()

         cursor.execute("select distinct starttime, count(d.observation_num), a.projectid from mwa_setting a \
         inner join mwa_project p on a.projectid = p.projectid \
         inner join data_files d on a.starttime = d.observation_num \
         where not exists (select 1 from data_files where filetype = 10 and a.starttime = data_files.observation_num) \
         and (select bool_and(remote_archived) from data_files where a.starttime = data_files.observation_num) \
         and starttime > 1061740896 and starttime < gpsnow()-900 \
         and a.mode = 'HW_LFILES' \
         and p.projectid not in ('C100', 'C001', 'D0004', 'G0002', 'C105', 'C106', 'D0000', 'D0003', 'D0005') \
         and dataquality <> 3 and dataquality <> 4 and dataquality <> 5 group by a.starttime order by starttime desc;")

         proj = ''
         first = True
         obs = []

         rows = cursor.fetchall()
         if rows:
            for r in rows:
               if first:
                  proj = r[2]
                  first = False
               elif proj != r[2]:
                  break
               
               obs.insert(0, (r[0], r[1]))

         return obs

      except Exception as e:
         raise e

      finally:
         if cursor:
            cursor.close()
            
         if con:
            self.dbp.putconn(conn=con)  


   def _getObsProcessingList(self):
      
      cursor = None
      con = None

      try:
         con = self.dbp.getconn()
         
         cursor = con.cursor()
         # only get the observations that have all their files transfered; 
         # only consider observations that are now()-900 secs to give the system time to consume all the file for that obs
         cursor.execute("select distinct starttime, count(d.observation_num) from mwa_setting a \
         inner join mwa_project p on a.projectid = p.projectid \
         inner join data_files d on a.starttime = d.observation_num \
         where not exists (select 1 from data_files where filetype = 10 and a.starttime = data_files.observation_num) \
         and (select bool_and(remote_archived) from data_files where a.starttime = data_files.observation_num) \
         and starttime > 1061740896 and starttime < gpsnow()-900 \
         and a.mode = 'HW_LFILES' \
         and p.projectid not in ('C100', 'C001', 'D0004', 'G0002', 'C105', 'C106', 'D0000', 'D0003', 'D0005') \
         and dataquality <> 3 and dataquality <> 4 and dataquality <> 5 group by a.starttime order by starttime asc;")
         
         #1056672016
         
         return cursor.fetchall()
         
      except Exception as e:
         raise e

      finally:
         if cursor:
            cursor.close()
            
         if con:
            self.dbp.putconn(conn=con)   
   
   
   def _getNGASFiles(self, obsid):
      files = []
      con = None
      cursor = None
   
      try:
         con = self.ngasdb.getconn()
         cursor = con.cursor()
         cursor.execute("select mount_point || '/' || file_name as path from ngas_files \
                        inner join ngas_disks on ngas_disks.disk_id = ngas_files.disk_id where file_id like %s \
                        and ngas_disks.disk_id in \
                        ('35ecaa0a7c65795635087af61c3ce903', '54ab8af6c805f956c804ee1e4de92ca4', \
                        '921d259d7bc2a0ae7d9a532bccd049c7', 'e3d87c5bc9fa1f17a84491d03b732afd')", [str(obsid) + '%fits'])
         
         return cursor.fetchall()
      
      finally:
         if cursor:
            cursor.close()
         
         if con:
            self.ngasdb.putconn(conn=con)
   
   
   def _pawseyMWAdmget(self, filename, host, port, timeout):
    
    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        val = struct.pack('>H', len(filename))
        val = val + filename
        # Connect to server and send data
        
        sock.connect((host, port))
        sock.sendall(val)
        # set the timeout
        sock.settimeout(timeout)
        # Receive return code from server
        return struct.unpack('!H', sock.recv(2))[0]
    finally:
        if sock:
            sock.close()
   
   
   def _processFlags(self, obsid, numfiles):
      
      try:
         
         if numfiles == 0:
            logger.info('_processFlags: %s does not have any files, ignoring')
            return
         
         self.handler.processFlags(obsid, numfiles)
         
         
      except Exception as e:
         logger.error("_processFlags: %s" % (str(e)))
         import traceback
         traceback.print_exc()

      finally:
         with self.plock:
            if obsid in self.processing:
               self.processing.remove(obsid)
            
            if self.pcount > 0:
               self.pcount -= 1
            
         self.sem.release()
      
      
   def stop(self):
      #if self.cmdserver:
      #   self.cmdserver.server_close()
      #   self.cmdthread.join()
      
      self.q.appendleft((None, None))

      self.resume()
      

   def pause(self):
      with self.pausecond:
         self.pausebool = True
         
      logger.info('pause called')

   
   def resume(self):
      with self.pausecond:
         if self.pausebool is True:
            self.pausebool = False
            self.pausecond.notify()
            
      logger.info('resume called')


   def start(self):
      
      while True:
       
         with self.pausecond:
            while self.pausebool is True:
               self.pausecond.wait(1)
       
         self.sem.acquire()
         
         with self.plock:
            obsid, numfiles = self.q.popleft()
            if obsid is not None:
               self.processing.append(obsid)
         
            logger.info('Queue size: %s Recent Queue Size: %s' % (str(len(self.q)), str(self.pcount)))
         
         if obsid is None:
            logger.info('Interrupted, shutting down flagger...')
            self.sem.release()
            sys.exit(2)
         
         t = threading.Thread(target=self._processFlags, args=(obsid, numfiles))
         t.start()



def main():
   
   logger.info('Starting flagger...')

   fh = DummyHandler()
   fp = FlagProcessor(fh)
   fp.start()
   
   logger.info('Stopped flagger')

if __name__ == "__main__":
   try:
      main()
      sys.exit(0)
   except Exception as e:
      logger.error(str(e))
      sys.exit(1)
