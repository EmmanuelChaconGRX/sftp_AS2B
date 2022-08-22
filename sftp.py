#!/usr/bin/env python

# Copyright (C) 2003-2007  Robey Pointer <robey@lag.net>
#
# This file is part of paramiko.
#
# Paramiko is free software; you can redistribute it and/or modify it under the
# terms of the GNU Lesser General Public License as published by the Free
# Software Foundation; either version 2.1 of the License, or (at your option)
# any later version.
#
# Paramiko is distrubuted in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with Paramiko; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA.

# based on code provided by raymond mosteller (thanks!)

import base64
import getpass
import os
import socket
import sys
import traceback
import ConfigParser
import fnmatch
import time

sys.path.append('C:\Python27\paramiko')

import paramiko
class SFTPClient2 (paramiko.SFTPClient):
    """
    SFTP client object.  C{SFTPClient} is used to open an sftp session across
    an open ssh L{Transport} and do remote file operations.
    """

    def __init__(self, sock):
        """
        Create an SFTP client from an existing L{Channel}.  The channel
        should already have requested the C{"sftp"} subsystem.

        An alternate way to create an SFTP client context is by using
        L{from_transport}.

        @param sock: an open L{Channel} using the C{"sftp"} subsystem
        @type sock: L{Channel}

        @raise SSHException: if there's an exception while negotiating
            sftp
        """
        paramiko.SFTPClient.__init__(self,sock)
        self.no_prefetch=True
        
    def get(self, remotepath, localpath, callback=None):
        """
        Copy a remote file (C{remotepath}) from the SFTP server to the local
        host as C{localpath}.  Any exception raised by operations will be
        passed through.  This method is primarily provided as a convenience.

        @param remotepath: the remote file to copy
        @type remotepath: str
        @param localpath: the destination path on the local host
        @type localpath: str
        @param callback: optional callback function that accepts the bytes
            transferred so far and the total bytes to be transferred
            (since 1.7.4)
        @type callback: function(int, int)

        @since: 1.4
        """
        file_size = self.stat(remotepath).st_size
        print 'Filesize: %d' % file_size
        fr = self.file(remotepath, 'rb')
        if False == self.no_prefetch:
           fr.prefetch()
        try:
            fl = file(localpath, 'wb')
            try:
                size = 0
                while True:
                    
                    data = fr.read(32768)
                    
                    if len(data) == 0:
                        break
                    fl.write(data)
                    size += len(data)
                    if callback is not None:
                        callback(size, file_size)
            finally:
                fl.close()
        finally:
            fr.close()
        s = os.stat(localpath)
        if s.st_size != size:
            raise IOError('size mismatch in get!  %d != %d' % (s.st_size, size))


def main():
  # setup logging
  try:
    config = ConfigParser.ConfigParser()
    config.read(sys.argv[1])
  except Exception, e:
    print "*** Error reading Configuration file\n"
    return 1

  try:
    paramiko.util.log_to_file(config.get('sftp', 'ftp_log'))
  except:
    ftp_log = 1
  try:
    import logging
    logging.basicConfig()
    logger = logging.getLogger('sftp')
    hdlr = logging.FileHandler(config.get('sftp', 'application_log'))
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)
    sftp_log(logger, "************ Application Starting.")
    log = 1
    year=time.strftime("%Y")
    month=time.strftime("%B")
   
  except:
    log = 0

  try:
    hostname = config.get('sftp', 'hostname')
  except Exception, e:
    if log:
      sftp_log(logger, "*** Hostname required.")
    return 1

  try:
    hostname = config.get('sftp', 'hostname')
  except Exception, e:
    if log:
      sftp_log(logger, "*** Hostname required.\n")
    return 1

  try:
    username = config.get('sftp', 'username')
  except Exception, e:
    username = ''
    
  try:
    password = config.get('sftp', 'password')
  except Exception, e:
    password = None
  try:
    keyfile = config.get('sftp', 'keyfile')
  except Exception, e:
    keyfile = None
  try:
    port = int(config.get('sftp', 'port'))
  except Exception, e:
    port = 22
  try:
    file_name_automatic = config.get('sftp', 'file_name_automatic')
  except Exception, e:
    file_name_automatic = '.'
  try:
    inbound_source = config.get('sftp', 'inbound_source')
    if file_name_automatic=='1':
        inbound_source=inbound_source+str(year)+'/'+month+'/'
  except Exception, e:
    inbound_source = '.'
  try:
    inbound_target = config.get('sftp', 'inbound_target')
    if file_name_automatic=='1':
        inbound_target=inbound_target+month
  except Exception, e:
    inbound_target = '.'
    
  try:
    outbound_source = config.get('sftp', 'outbound_source')
  except Exception, e:
    outbound_source = '.'

  try:
    outbound_target = config.get('sftp', 'outbound_target')
  except Exception, e:
    outbound_target = '.'

  try:
    inbound_backup = config.get('sftp', 'inbound_backup')
  except Exception, e:
    inbound_backup = ''

  try:
    outbound_backup = config.get('sftp', 'outbound_backup')
  except Exception, e:
    outbound_backup = ''

  try:
    outbound_filename = config.get('sftp', 'outbound_filename')
  except Exception, e:
    outbound_filename = '*'

  try:
    inbound_filename = config.get('sftp', 'inbound_filename')
  except Exception, e:
    inbound_filename = '*'
    
  try:
    stat_after_upload = config.get('sftp','stat_after_upload')
    stat_after_upload = stat_after_upload.lower() in ('true','yes''y','1')
  except Exception, e:
    stat_after_upload = True

  try:
    disable_prefetch = config.get('sftp','disable_prefetch')
  except Exception, e:
    disable_prefetch = False
  
  if not os.path.exists(inbound_target):
    try:  
       os.mkdir(inbound_target)
    except OSError:  
       if log:
          sftp_log(logger, "Creation of the directory %s failed" % inbound_target)
       return 1	 
  
  
  # get host key, if we know one
  hostkeytype = None
  hostkey = None
  try:
    host_keys = paramiko.util.load_host_keys(os.path.expanduser('~/.ssh/known_hosts'))
  except IOError:
    try:
      # try ~/ssh/ too, because windows can't have a folder named ~/.ssh/
      host_keys = paramiko.util.load_host_keys(os.path.expanduser('~/ssh/known_hosts'))
    except IOError:
      host_keys = {}

  if host_keys.has_key(hostname):
    hostkeytype = host_keys[hostname].keys()[0]
    hostkey = host_keys[hostname][hostkeytype]
    print 'Using host key of type %s' % hostkeytype
    print 'Using host key %s:' % hostkey
	
  

  # now, connect and use paramiko Transport to negotiate SSH2 across the connection
  try:
    print "Prefetch Disabled: %s" % disable_prefetch
    try:
      t = paramiko.Transport((hostname, port))
    except:
      if log:
        sftp_log(logger, "*** Can't connect to "+hostname+" on port "+str(port)+".")
      return 1
    try:
      if keyfile != None:
          t.connect(username=username, key_filename=keyfile, hostkey=hostkey)
      else:
          t.connect(username=username, password=password, hostkey=hostkey)
      sftp = SFTPClient2.from_transport(t)
      sftp.no_prefetch=disable_prefetch
    except:
      t.close()
      if log:
        sftp_log(logger, "*** Authentication Failure.")
      return 1
      
    # dirlist on remote host
    #print "Inbound Source:", sftp.listdir(inbound_source)
    #print "Inbound Target:", os.listdir(inbound_target)
    try:
      inboundfiles = fnmatch.filter(sftp.listdir(inbound_source), inbound_filename)
    except:
      t.close()
      if log:
        sftp_log(logger, "*** Inbound Source folder does not exist")
      return 1
    for file in inboundfiles:
      print file
      try:
        sftp.get(inbound_source+'/'+file, inbound_target+'/'+file)
        sftp_log(logger, "*** Successfully downloaded "+inbound_source+'/'+file+' from remote server')
        filesuccess = 1
      except:
        sftp_log(logger, "*** Error downloading "+inbound_source+'/'+file+' from remote server')
        filesuccess = 0
      if filesuccess:
        try:
          sftp.remove(inbound_source+'/'+file)
          sftp_log(logger, "*** Successfully removed "+inbound_source+'/'+file+' from remote server')
        except:
          sftp_log(logger, "*** Error removing "+inbound_source+'/'+file+' from remote server')

    if outbound_source != '.':
      try:
        outboundfiles = fnmatch.filter(os.listdir(outbound_source), outbound_filename)
      except:
        t.close()
        if log:
          sftp_log(logger, "*** Outbound Source folder does not exist")
        return 1
      for file in outboundfiles:
        print file
        try:
          sftp.put(outbound_source+'/'+file, outbound_target+'/'+file,None,stat_after_upload)
          sftp_log(logger, "*** Successfully uploaded "+outbound_source+'/'+file+' to remote server')
          filesuccess = 1
        except Exception as e:
          sftp_log(logger, "AQUIIIIIIIII")
          sftp_log(logger, "*** Error uploading "+outbound_source+'/'+file+' to '+outbound_target+'/'+file )
          print e
          filesuccess = 0
        if filesuccess:
          try:
            os.remove(outbound_source+'/'+file)
            sftp_log(logger, "*** Successfully removed "+outbound_source+'/'+file+' from local folder')
          except:
            sftp_log(logger, "*** Error removing "+outbound_source+'/'+file+' from local folder')

    t.close()

  except Exception, e:
    if log:
      sftp_log(logger, '*** Caught exception: %s: %s' % (e.__class__, e))
    traceback.print_exc()
    try:
      t.close()
    except:
      pass
    return 1
def sftp_log(logger,text):
  print text + "\n"
  logger.info(text)

if __name__ == "__main__":
  sys.exit(main())
