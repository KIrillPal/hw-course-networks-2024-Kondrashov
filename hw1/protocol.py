import socket
import collections
from threading import Thread
from time import sleep
import struct
from dataclasses import dataclass
from random import random


class UDPBasedProtocol:
    def __init__(self, *, local_addr, remote_addr):
        self.udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.remote_addr = remote_addr
        self.local_addr = local_addr
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
            send_timeout: float = 0.0001, # in seconds
            recv_timeout: float = 0.00001, # in seconds
            max_buffer_size: int = 1 << 24,  # in bytes
            max_packet_size: int = (1 << 16) - 32, # in bytes
            resend_ack_ratio: float = 0.05,
            *args, 
            **kwargs
        ):
        super().__init__(*args, **kwargs)

        self.seq = 0
        self.ack = 0
        self.is_open = True
        self.send_timeout = send_timeout
        self.recv_timeout = recv_timeout
        self.max_buffer_size = max_buffer_size
        self.max_packet_size = max_packet_size
        self.resend_ack_ratio = resend_ack_ratio
        self.header_size = 16 # in bytes
        self.max_data_size = self.max_packet_size - self.header_size

        # Set up buffer
        self.buffer = bytearray()
        self.buffer_start = 0

        # Run listener
        self.udp_socket.setblocking(False)
        self.listener = Thread(target=self._listen)
        self.listener.start()


######################
### Public methods ###
######################


    def send(self, data: bytes) -> int:
        start_seq = self.seq
        while self.seq < start_seq + len(data):
            window_size = self._get_window_size()
            pos = self.seq - start_seq

            packet = self.Packet(
                self.seq, 
                self.ack, 
                data[pos:pos+window_size]
            )
            packet_bytes = self._pack(packet)
            self.sendto(packet_bytes)
            
            sleep(self.send_timeout)
        return len(data)


    def recv(self, n: int) -> bytes:
        # Spinlock to get all data
        while len(self.buffer) - self.buffer_start < n and self.is_open:
            sleep(self.recv_timeout)
        received = self._pop_from_buffer(n)
        #print("Finally got", len(received))
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
        data : bytes = bytes()


    def _get_packet_size(self, packet : Packet):
        return self.header_size + len(packet.data)


    def _unpack(self, packet : bytes) -> Packet:
        seq, ack = struct.unpack("!QQ", packet[:self.header_size])
        data = packet[self.header_size:]
        return self.Packet(seq, ack, data)
    

    def _pack(self, packet : Packet) -> bytes:
        header = struct.pack("!QQ", packet.seq, packet.ack)
        return header + packet.data
    

    def _listen(self):
        while self.is_open:
            packet_bytes = self._wait_for_packet()
            if len(packet_bytes) < self.header_size:
                continue
            packet = self._unpack(packet_bytes)
            #print(self.seq, self.ack, packet.seq, packet.ack)
            
            if self.seq < packet.ack:
                self.seq = packet.ack
                #print("Seq", self.seq)
                
            if self.ack != packet.seq:
                if self.ack > packet.seq and random() <= self.resend_ack_ratio:
                    self._confirm_receipt(0)
                continue

            #print("Recv", len(packet_bytes), "bytes")
            if len(packet.data) > 0:
                self._confirm_receipt(
                    self._push_to_buffer(packet.data)
                )



    def _wait_for_packet(self) -> bytes:
        # Spinlock
        while self.is_open:
            try:
                return self.recvfrom(self.max_packet_size)
            except socket.error:
                sleep(self.recv_timeout)
        return bytes()
        

    def _get_window_size(self):
        # Always max possible window
        return self.max_data_size
    
    
    def _push_to_buffer(self, data : bytes) -> int:
        # Try to clean buffer
        if len(self.buffer) + len(data) > self.max_buffer_size:
            del self.buffer[:self.buffer_start]
            self.buffer_start = 0
        # Try to push
        if len(self.buffer) + len(data) <= self.max_buffer_size:
            self.buffer.extend(data)
            return len(data)
        return 0
    

    def _pop_from_buffer(self, n : int) -> bytes:
        data = self.buffer[self.buffer_start:self.buffer_start+n]
        self.buffer_start += n
        return bytes(data)


    def _confirm_receipt(self, n : int):
        self.ack += n
        acknowledgement = self.Packet(self.seq, self.ack)
        packet_bytes = self._pack(acknowledgement)
        self.sendto(packet_bytes)