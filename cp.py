#!/usr/bin/python3.11
import os
import mmap
import shutil
import sys

move = os.SPLICE_F_MOVE if os.getenv("MOVE") else 0
more = os.SPLICE_F_MORE if os.getenv("MORE") else 0
flag = move | more


def copy(src_path: str, dst_path: str, chunk_size: int = 1 << 20) -> None:
    src_fd = os.open(src_path, os.O_RDWR)
    dst_fd = os.open(dst_path, os.O_RDWR | os.O_CREAT | os.O_TRUNC)
    fsize = os.stat(src_path).st_size

    match (sys.argv[3] if len(sys.argv) > 3 else "--splice"):
        case "--copy-file-range":
            os.copy_file_range(src_fd, dst_fd, fsize)
        case "--sendfile":
            os.sendfile(dst_fd, src_fd, 0, fsize)
        case "--splice":
            # create a pipe for our buffer instead of using python memory
            r_fd, w_fd = os.pipe()
            while True:
                # transfer data from src to the write end of the pipe
                # it stays in the kernel
                bytes_in = os.splice(src_fd, w_fd, chunk_size, flags=flag)
                if bytes_in == 0:  # EOF
                    break
                # transfer data from the read end of the pipe to dst
                # it doesn't go through userspace
                bytes_out = os.splice(r_fd, dst_fd, bytes_in, flags=flag)
        case "--copyfileobj":
            shutil.copyfileobj(
                os.fdopen(src_fd, "rb"), os.fdopen(dst_fd, "wb"), length=chunk_size
            )
        case "--mmap-memcpy":
            src_mv = memoryview(mmap.mmap(src_fd, fsize))
            os.ftruncate(dst_fd, fsize)
            dst_mv = memoryview(mmap.mmap(dst_fd, fsize))
            dst_mv[:] = src_mv[:]
        case "--mmap-memcpy-chunked":
            src_mv = memoryview(mmap.mmap(src_fd, fsize))
            os.ftruncate(dst_fd, fsize)
            dst_mv = memoryview(mmap.mmap(dst_fd, fsize))
            for start in range(0, fsize, chunk_size):
                end = min(start + chunk_size, fsize)
                dst_mv[start:end] = src_mv[start:end]


if __name__ == "__main__":
    copy(sys.argv[1], sys.argv[2])
