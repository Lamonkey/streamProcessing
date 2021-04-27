data = 'abcdefg'
to_be_sent = data.encode('utf-8')

import socket
# Create a socket with the socket() system call
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)    
# Bind the socket to an address using the bind() system call
s.bind(('192.168.50.232', 9999))
# Enable a server to accept connections. 
# It specifies the number of unaccepted connections 
# that the system will allow before refusing new connections
s.listen(1)
# Accept a connection with the accept() system call
conn, addr = s.accept()                                 
# Send data. It continues to send data from bytes until either
# all data has been sent or an error occurs. It returns None.
conn.sendall(to_be_sent)                                
conn.close()