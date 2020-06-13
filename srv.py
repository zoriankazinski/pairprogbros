import json
import socket
import threading

class Server:
  
  username = 'USERNAME'
  password = 'PASSWORD'

  def __init__(self):
    self.srv = self._gen_server()
    threading.Thread(self._start_server()).start()

  def _gen_server(self,port=5100):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(('',port))
    srv.listen()
    return srv

  def _clear_server(self):
    self.srv.close()

  def _start_server(self):
    while True:
      sock, addr = self.srv.accept()
      t = threading.Thread(target=self._serve,args=(sock,))
      t.start()

  def _serve(self,sock):
    auth = self._auth(sock)
    while auth:
      req  = sock.recv(65536)
      resp = self._handle(req)
      sock.send(resp)
    sock.close()

  def _auth(self,sock):
    sock.settimeout(5.0)
    username = sock.recv(65536)
    password = sock.recv(65536)
    sock.settimeout(None)
    try:
      username = username.decode().strip()
      password = password.decode().strip()
    except:
      pass
    if self.username == username and self.password == password:
      return True
    return False
   
  def _handle(self,data):
    try:
      j_data = json.loads(data)
      return b'OK\n'
    except:
      return b'ERROR\n'

Z = Server()
