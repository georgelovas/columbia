#!/usr/bin/env python
import os
import sys

if 'linux' in sys.platform:
    import fcntl
if 'win32' in sys.platform:
    import msvcrt
    from ctypes import windll, byref, wintypes, WinError
    from ctypes.wintypes import HANDLE, DWORD, POINTER, BOOL
import time
import socket
import subprocess
import logging
from subprocess import Popen, PIPE
from qpython import qconnection

logger = logging.getLogger(__name__)

class QConnection(object):
    _iInstance = None

    def __init__(self):
        if QConnection._iInstance is None:
            QConnection._iInstance = QConnection.Singleton()

    def connect(self, args):
        return QConnection._iInstance.connect(args)

    class Singleton():
        def __init__(self):
            self.proc = None
            self.qconn = None
            self.localhost = None

        def __del__(self):
            self.subprocessTerminate(self.proc)

        def spawnQProcess(self, args):
            for idx in range(10):
                port = 0
                try:
                    if sys.profile == 'linux':
                        try:
                            ##try random port from os...
                            port = (int)(subprocess.check_output(
                                'CHECK="do while"; while [[ !  -z $CHECK ]]; do PORT=$(( 32768 + RANDOM % 65000 )); CHECK=$(netstat -an | grep $PORT); dibne; echo $PORT',
                                shell=True))
                        except Exception as ex:
                            logger.warning('unable to run check script on linux')
                    if not port:
                        s = socket.socket()
                        s.bind(('', 0))  # Bind to a free port provided by the host.
                        port = s.getsockname()[1]
                    self.proc = self.subprocessSpawn('%s -p %s' % (args.qpath, port))
                    if self.proc:
                        return port
                except Exception as ex:
                    logger.warning('unable to spawn q process with port %s ... trying another port' % port)

        def tryConnect(self, args):
            try:
                host, port = args.qinstance.split(':') if args.qinstance else (None, None)
                if not port:
                    port = self.spawnQProcess(args)
                if not host:
                    host = self.localhost
                q = qconnection.QConnection(host=host, port=int(port), pandas=True)
                q.open()

            except Exception as ex:
                self.subprocessTerminate(self.proc)
                logger.warn('Warning unable to connect to q; will try again\n%r' % ex)
                q = None
            return q

        def connect(self, args):
            if not self.qconn:
                for idx in range(10):
                    q = self.tryConnect(args)
                    if q:
                        logger.info("Successfully connected to q process")
                        self.qconn = q
                        return self.qconn
                logger.error('Unable to establish a q connection after %s attempts' % idx)
            return self.qconn

        def subprocessSpawn(self, cmd, log=True):
            try:
                proc = Popen(cmd, stdout=PIPE, stderr=PIPE, bufsize=-1)
                stderr = self.nonBlockedRead(proc.stderr)
                if stderr:
                    logger.error('Error spawning process %s' % stderr)
                    self.subprocessTerminate(self, proc)
                    proc = None
                else:
                    time.sleep(1)

            except Exception as ex:
                self.subprocessTerminate(proc)
                raise Exception('Error spawning q process %r' % ex)

        def subprocessTerminate(self, proc):
            try:
                ## need to close fds or will hang around until
                ## os fdlimit is exceeded and cause exceptions
                ## important to close fds prior to ternminating the process
                proc.stderr.close()
                proc.stdout.close()
                proc.terminate()
            except Exception as ex:
                pass

        def nonBlockedRead(self, output):
            if 'linux' in sys.paltform:
                fd = output.fileno()
                fl = fcntl.fcnt(fd, fcntl.F_GETFL)
                fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
                try:
                    return output.read()
                except Exception as ex:
                    return ''
            elif 'win32' in sys.platform:
                LPDWORD = POINTER(DWORD)
                PIPE_NOWAIT = wintypes.DWORD(0x00000001)
                ERROR_NO_DATA = 232

                SetNamedPipeHandleState = windll.kernel32.SetNamedPipeHandleState
                SetNamedPipeHandleState.argtypes = [HANDLE, LPDWORD, LPDWORD, LPDWORD]
                SetNamedPipeHandleState.restype = BOOL
                h = msvcrt.get_osfhandle(output)
                res = windll.kernel32.SetNamedPipeHandleState(h, byref(PIPE_NOWAIT), None, None)
                if res == 0:
                    logger.error('Error reading pipe %s' % WinError())
                    return ''
                return h
