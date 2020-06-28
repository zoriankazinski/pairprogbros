import sys
import socket
import fcntl
import struct
import array

def ifconfig(max_possible=128):
  bytes = max_possible * 32
  names = array.array('B', b'\0' * bytes)
  names_memory_addr = names.buffer_info()[0]
  st = struct.pack('iL', bytes, names_memory_addr)
  sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  s_fno = sock.fileno()
  outbytes = struct.unpack('iL',fcntl.ioctl(s_fno,0x8912,st))[0]
  lst = {}
  names_s = names.tobytes()
  for i in range(0,outbytes,40):
    name = names_s[i:i+16].split(b'\0',1)[0].decode()
    ip = '.'.join(map(str,names_s[i+20:i+24]))
    lst[name] = ip
  return lst

if __name__ == "__main__":
  command = sys.argv[1]
  func = locals()[command]
  print(func())
