import threading
import socket
import enum
import os

class Config(enum.Enum):
    home = 'C:/'

class WEB:

    def __init__(self,port=1996):
        self.sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.sock.bind(('',port))
        self.sock.listen(5)

    def __call__(self):
        self._start()

    def _start(self):
        while True:
            sock, addr = self.sock.accept()
            t = threading.Thread(target=self.answer,args=(sock,))
            t.start()

    def answer(self,sock):
        print("### -> Awaiting New Connections...")
        data = sock.recv(64000)
        print("### -> Received A New Request!")
        try:
            print('         \n'.join(data.decode().splitlines()))
        except:
            print(data)
        A = Response(Request(data))
        print("### -> Generated Response, Sending...")
        sock.send(A())
        print("### -> Closing Connection.")
        sock.close()

class Request:

    def __init__(self,request_bytes):
        self.request_lines = request_bytes.decode().splitlines()
        self.Headers = self.header_factory(self.request_lines[1:])

    @property
    def type(self):
        return self.request_lines[0].split(' ')[0]

    @property
    def route(self):
        return self.request_lines[0].split(' ')[1]

    @property
    def version(self):
        return self.request_lines[0].split(' ')[2]

    @property
    def headers(self):
        return self.Headers

    @property
    def headers_dict(self):
        return self.Headers.__dict__

    @staticmethod
    def header_factory(header_lines):
        class Headers:
            def __init__(self,headers):
                for line in headers:
                    arg, val = line.split(':')[0], ': '.join(line.split(': ')[1:])
                    if arg:
                        setattr(self,arg,val)
        return Headers(header_lines)

class Response:

    def __init__(self,request):
        self.request = request
        self.status_code = [ "Success", '200' ]

    def __call__(self):
        return '\n'.join([self.status_line,self.headers,'']).encode()+self.body

    @property
    def version(self):
        return self.request.version

    @property
    def code(self):
        return self.status_code[1]

    @property
    def text(self):
        return self.status_code[0]

    @property
    def status_line(self):
        return ' '.join([self.version,self.code,self.text])

    @property
    def headers(self):
        return ''

    @property
    def body(self):
        try:
            if self.request.route != '/':
                return self.read_html_file(self.request.route)
            else:
                return self.read_html_file('index.html')
        except Exception as e:
            self.status_code = ["Not Found","404"]
            return b' '

    @staticmethod
    def read_html_file(file):
        filepath = os.path.join(Config.home.value,file)
        with open(filepath,'rb') as fd:
            return fd.read()

A = WEB()
A()
