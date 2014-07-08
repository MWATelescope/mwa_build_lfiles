#!/usr/bin/env python
import io, sys, os, glob
import optparse
import subprocess, shlex
import base64


ARCHIVE_CLIENT_PATH = '/home/dpallot/repos/ngas/src/ngamsCClient/'
ARCHIVE_CLIENT_NAME = 'ngamsCClient'
ARCHIVE_CLIENT_SERVER = 'mwa-pawsey01.pawsey.ivec.org'
ARCHIVE_CLIENT_PORT = '7777'

DBHOST = '202.9.9.4'


def tar_up_flag_files(dir, obsid):
    
    if dir.endswith('/') is False:
        dir = dir + '/'

    name =  obsid + "_flags.zip"
    
    os.chdir(dir)
    proc = subprocess.Popen(["zip", "-R", name, obsid + "*.mwaf"], stdout=subprocess.PIPE)

    out, err = proc.communicate()
    exitcode = proc.returncode
    
    if exitcode != 0:
        raise Exception(out)
    
    print out

    fullpath = os.getcwd() + '/' + name

    return fullpath, name, os.stat(name).st_size


def archive_file(fullpath, size):
    proc = subprocess.Popen([ARCHIVE_CLIENT_PATH + ARCHIVE_CLIENT_NAME, "-host", ARCHIVE_CLIENT_SERVER, "-port", ARCHIVE_CLIENT_PORT, "-auth", "bmdhc21ncjpuZ2FzJGRiYQ==", \
                            "-cmd", "QARCHIVE", "-mimeType", "application/octet-stream", "-fileUri", fullpath], stdout=subprocess.PIPE)
    
    out, err = proc.communicate()
    exitcode = proc.returncode
    
    if exitcode != 0:
        raise Exception(out)
    
    print out
        
    #/scratch/mwaops/ngamsCClient -host 146.118.84.64 -port 7777 -auth bmdhc21ncjpuZ2FzJGRiYQ== -cmd QARCHIVE -mimeType application/octet-stream -fileUri <full_path_flagg_file>

def check_MC_database(obsid, filename):
    sql = "SELECT * from data_files where observation_num = %s AND filename = \'%s\';" % (str(obsid), filename)
    
    proc = subprocess.Popen(["psql", "-t", "-A","-U", "mwa", "-h", DBHOST, "mwa", "-c", sql], stdout=subprocess.PIPE)
    
    out, err = proc.communicate()
    exitcode = proc.returncode
    
    if exitcode != 0:
        raise Exception(out)
    
    print out
 
    # returned a rowset; if so then entry for the filename exists
    if len(out) > 0:
        return True

    return False
    
    
def update_MC_database(obsid, filename, size):
    
    uri = 'http://mwangas/RETRIEVE?file_id=' + filename
    sql = "INSERT INTO data_files (observation_num, filetype, size, filename, site_path, remote_archived) VALUES (%s, %s, %s, \'%s\', \'%s\', TRUE)" % (str(obsid), str(10), str(size), filename, uri)
    
    proc = subprocess.Popen(["psql", "-U", "mwa", "-h", DBHOST, "mwa", "-c", sql], stdout=subprocess.PIPE)
    
    out, err = proc.communicate()
    exitcode = proc.returncode
    
    if exitcode != 0:
        raise Exception(out)
    
    print out
    
            
def main(argv):

    parser = optparse.OptionParser(usage='Takes flag files in a certain directory *.mwaf, zips em up, pushes the container to Pawsey then updates the M&C db.')
    parser.add_option('-d', action="store", dest='dir', help='Directory containing flag files')
    parser.add_option('-o', action="store", dest='obsid', help='ObsID of observation')
    
    options, args = parser.parse_args()

    if options.dir is None:
        raise Exception("directory missing")
        
    if options.obsid is None:
        raise Exception("obsid missing")
    
    
    path, name, size = tar_up_flag_files(options.dir, options.obsid)
    
    archive_file(path, size)

    # only insert in M&C database if entry for that file does not exists in data_files. Happy for NGAS to hold multiple versions of the same file. 
    if check_MC_database(options.obsid, name) == False:
        update_MC_database(options.obsid, name, size)


if __name__ == "__main__":
    try:
        main(sys.argv)
        sys.exit(0)
    except Exception as e:
        print str(e)
        sys.exit(-1)
