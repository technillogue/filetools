#!/bin/python3.11
import fcntl
import os
import random
import socket
import sys
import time
from threading import Event, Thread

try:
    with open("/proc/sys/fs/pipe-max-size") as pipe_file:
        max_pipe_size = int(pipe_file.read())
        print("max_pipe_size", max_pipe_size)
except OSError:
    max_pipe_size = 1024 * 1024

chunk_size = 1 << 20


def broadcast_service(port: int) -> Event:
    # Start broadcasting service in a new thread
    event = Event()
    event.set()

    def _broadcast_service(port: int, interval: int = 2) -> None:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        server_ip = s.getsockname()[0]
        s.close()

        broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        message = f"{server_ip}:{port}".encode("utf-8")

        while event.wait():
            broadcast_socket.sendto(message, ("<broadcast>", 12346))
            print("sent service broadcast: ", message)
            time.sleep(interval)

    broadcaster = Thread(target=_broadcast_service, args=(port,))
    broadcaster.daemon = True
    broadcaster.start()
    return event


def discover_service() -> tuple[str, int]:
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    client_socket.bind(("", 12346))
    while True:
        print("waiting for service broadcast")
        message, _ = client_socket.recvfrom(1024)
        server_ip, server_port = message.decode("utf-8").split(":")
        print("received ip broadcast", server_ip)
        return server_ip, int(server_port)


def server(src_path: str) -> None:
    src_fd = os.open(src_path, os.O_RDWR)
    fsize = os.stat(src_path).st_size
    do_broadcast = server_sock = None
    port = 1337
    try:
        do_broadcast = broadcast_service(port)
        while 1:
            server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_sock.bind(("0.0.0.0", port))
            server_sock.listen(1)
            sent = 0
            sock = None
            try:
                sock, _ = server_sock.accept()
                server_sock.close()
                do_broadcast.clear()
                t = time.perf_counter()
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, chunk_size)
                while sent < fsize:
                    print("sendfile going")
                    sent += os.sendfile(sock.fileno(), src_fd, sent, fsize - sent)
                    print("sent chunk", fsize - sent, "left to send")
            finally:
                sock and sock.close()
                do_broadcast.set()
            print(f"{fsize / (time.perf_counter() - t) / 1024 / 1024:.5} MB/s")
    finally:
        do_broadcast and do_broadcast.clear()
        server_sock and server_sock.close()

def sample_print(*msg: str) -> None:
    if random.random() > 0.99:
        print(*msg)

def client(dst_path: str) -> None:
    flags = os.O_RDWR | os.O_CREAT | os.O_TRUNC
    dst_fd = os.open(dst_path, flags)
    remote = discover_service()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(remote)
    t = time.perf_counter()
    # we'll use this to buffer data
    r_fd, w_fd = os.pipe()
    if sys.platform == "linux":
        # make the pipe bigger
        fcntl.fcntl(w_fd, fcntl.F_SETPIPE_SZ, max_pipe_size)
        # pipe_flags = fcntl.fcntl(w_fd, fcntl.F_GETFL, 0) | os.O_NONBLOCK
        # fcntl.fcntl(w_fd, fcntl.F_SETFL, pipe_flags)
        chunk_size = max_pipe_size
    # make the receive buffer bigger
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, chunk_size)
    fsize = 0
    chunks = 0
    buffered = 0
    splice_flag = os.SPLICE_F_MOVE if os.getenv("MOVE") else 0 
    should_drain = Event()

    def drain_pipe():
        while not should_drain.is_set():
            sample_print("drain", os.splice(r_fd, dst_fd, chunk_size, flags=splice_flag) / 1024)

    drain_thread = Thread(target=drain_pipe)
    drain_thread.start()
    try:
        while True:
            # transfer data from src to the write end of the pipe
            # it stays in the kernel
            bytes_in = os.splice(sock.fileno(), w_fd, chunk_size, flags=splice_flag)
            if bytes_in == 0:  # EOF
                break
            sample_print("received", bytes_in / 1024, "kb")
            chunks += 1
            fsize += bytes_in
            buffered += bytes_in
            # if we only received a little data, wait until the pipe is full
            # if buffered >= chunk_size - 1:
            #     # transfer data from the read end of the pipe to dst
            #     # it doesn't go through userspace
            #     print("splice out", buffered)
            #     buffered -= os.splice(r_fd, dst_fd, buffered, flags=splice_flag)
            #     if buffered != 0:
            #         print("incomplete splice to disk")
    finally:
        sock.close()
        should_drain.set()
        print(f"{fsize / (time.perf_counter() - t) / 1024 / 1024:.5} MB/s, {chunks} chunks")
        drain_thread.join()


if __name__ == "__main__":
    if sys.argv[1] == "--send":
        server(sys.argv[2])
    elif sys.argv[1] == "--recv":
        client(sys.argv[2])
