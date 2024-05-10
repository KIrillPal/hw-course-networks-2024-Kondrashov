import socket
import collections
from threading import Thread
from time import sleep
import struct
from dataclasses import dataclass


class UDPBasedProtocol:
    def __init__(self, *, local_addr, remote_addr):
        self.udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.remote_addr = remote_addr
        self.udp_socket.bind(local_addr)

    def sendto(self, data):
        return self.udp_socket.sendto(data, self.remote_addr)

    def recvfrom(self, n):
        msg, addr = self.udp_socket.recvfrom(n)
        return msg

    def close(self):
        self.udp_socket.close()


class MyTCPProtocol(UDPBasedProtocol):

    def __init__(
            self, 
            waiting_timeout: float = 0.001, # in seconds
            max_buffer_size: int = 1 << 18,  # in bytes
            max_packet_size: int = 1500, # in bytes
            *args, 
            **kwargs
        ):
        super().__init__(*args, **kwargs)

        self.seq = 0
        self.ack = 0
        self.is_open = True
        self.timeout = waiting_timeout
        self.MAX_BUFFER_SIZE = max_buffer_size
        self.MAX_packet_SIZE = max_packet_size
        self.HEADER_SIZE = 16 # in bytes
        self.MAX_DATA_SIZE = self.MAX_packet_SIZE - self.HEADER_SIZE

        # Set up buffer
        self.buffer = collections.deque(maxlen=self.MAX_BUFFER_SIZE)

        # Run listener
        self.udp_socket.setblocking(False)
        self.listener = Thread(target=self._listen)
        self.listener.start()


######################
### Public methods ###
######################

    def send(self, data: bytes):
        start_seq = self.seq
        while self.seq < start_seq + len(data):
            pos = self.seq - start_seq
            window_size = self._get_window_size()
            packet = self.Packet(self.seq, self.ack, data[pos:pos+window_size])
            packet_bytes = self._pack(packet)
            print("Send", len(packet_bytes), "bytes")
            self.sendto(packet_bytes)
            sleep(self.timeout)


    def recv(self, n: int):
        # Spinlock to get all data
        while len(self.buffer) < n and self.is_open:
            sleep(self.timeout)
            
        received = self.buffer[:n]
        self.buffer = self.buffer[n:]
        return received


    def close(self):
        super().close()
        self.is_open = False
        self.listener.join()


#######################
### Support methods ###
#######################

    
    @dataclass
    class Packet:
        seq : int
        ack : int
        data : bytes = None


    def _get_packet_size(self, packet : Packet):
        return self.HEADER_SIZE + len(packet.data)


    def _unpack(self, packet : bytes) -> Packet:
        seq, ack = struct.unpack("!QQ", packet[:self.HEADER_SIZE])
        data = packet[self.HEADER_SIZE:]
        return self.Packet(seq, ack, data)
    

    def _pack(self, packet : Packet) -> bytes:
        header = struct.pack("!QQ", packet.seq, packet.ack)
        return header + packet.data
    

    def _listen(self):
        while self.is_open:
            packet_bytes = self._wait_for_packet()
            print("Recv", len(packet_bytes), "bytes")
            packet = self._unpack(packet_bytes)

            if self.ack > packet.seq:
                continue

            if self.ack < packet.seq:
                self._confirm_receipt(0)
                continue
            
            if self.seq < packet.ack:
                self.seq = packet.ack

            self._confirm_receipt(
                self._push_to_buffer(packet.data)
            )


    def _wait_for_packet(self) -> bytes:
        # Spinlock
        while self.is_open:
            try:
                return self.recvfrom(self.MAX_packet_SIZE)
            except socket.error:
                sleep(self.timeout)
        return bytes()
    

    def _wait_for_window_space(self, to_be_sent : int):
        # Spinlock
        while to_be_sent - self.seq > self._get_window_size() and self.is_open:
            sleep(self.timeout)
        

    def _get_window_size(self):
        # Always max possible window
        return self.MAX_DATA_SIZE
    
    
    def _push_to_buffer(self, data : bytes) -> int:
        if len(self.buffer) + len(data) <= self.MAX_BUFFER_SIZE:
            self.buffer.extend(data)
            return len(data)
        return 0


    def _confirm_receipt(self, n : int):
        self.ack += n
        acknowledgement = self.Packet(self.seq, self.ack)
        self.sendto(self._pack(acknowledgement))