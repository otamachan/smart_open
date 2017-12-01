# -*- coding: utf-8 -*-
"""Implements file-like objects for reading and writing from/to S3."""
import boto3

import io
import logging

import six

from six.moves import queue
import threading

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


START = 0
CURRENT = 1
END = 2
WHENCE_CHOICES = (START, CURRENT, END)

DEFAULT_READ_BUFFER_SIZE = 20 * 1024**2
"""Default read buffer size"""
DEFAULT_MIN_PART_SIZE = 50 * 1024**2
"""Default minimum part size for S3 multipart uploads"""
MIN_MIN_PART_SIZE = 5 * 1024 ** 2
"""The absolute minimum permitted by Amazon."""
READ = 'r'
READ_BINARY = 'rb'
WRITE = 'w'
WRITE_BINARY = 'wb'
MODES = (READ, READ_BINARY, WRITE, WRITE_BINARY)
"""Allowed I/O modes for working with S3."""


def _range_string(start, stop=None):
    #
    # https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
    #
    if stop is None:
        return 'bytes=%d-' % start
    return 'bytes=%d-%d' % (start, stop)


def _clamp(value, minval, maxval):
    return max(min(value, maxval), minval)


def open(bucket_id, key_id, mode, **kwargs):
    logger.debug('%r', locals())
    if mode not in MODES:
        raise NotImplementedError('bad mode: %r expected one of %r' % (mode, MODES))

    encoding = kwargs.pop("encoding", "utf-8")
    errors = kwargs.pop("errors", None)
    newline = kwargs.pop("newline", None)
    line_buffering = kwargs.pop("line_buffering", False)
    s3_min_part_size = kwargs.pop("s3_min_part_size", DEFAULT_MIN_PART_SIZE)

    if mode in (READ, READ_BINARY):
        fileobj = BufferedInputBase(bucket_id, key_id, **kwargs)
    elif mode in (WRITE, WRITE_BINARY):
        fileobj = BufferedOutputBase(bucket_id, key_id, min_part_size=s3_min_part_size, **kwargs)
    else:
        assert False

    if mode in (READ, WRITE):
        return io.TextIOWrapper(fileobj, encoding=encoding, errors=errors,
                                newline=newline, line_buffering=line_buffering)
    elif mode in (READ_BINARY, WRITE_BINARY):
        return fileobj
    else:
        assert False


class RawReader(object):
    """Read an S3 object."""
    def __init__(self, s3_object):
        self._object = s3_object
        self._content_length = self._object.content_length
        self._buffer_pos = 0
        self._buffer = b""
        self._request = queue.Queue()
        self._lock = threading.Lock()
        self._thread = threading.Thread(target=self._run)
        self._thread.daemon = True
        self._thread.start()

    def read(self, pos, size=-1):
        while True:
            self._request.join()
            with self._lock:
                if self._buffer_pos <= pos and \
                   pos < self._buffer_pos + len(self._buffer):
                    if not self._eof:
                        # start next reading here
                        self._request.put((self._buffer_pos + len(self._buffer), size))
                    return (self._buffer[(pos - self._buffer_pos):],
                            pos,
                            self._eof)
                else:
                    self._request.put((pos, size))

    def _run(self):
        while True:
            pos, size = self._request.get()
            if size <= 0:
                end = None
            else:
                end = min(self._content_length, pos + size)
            buf = self._object.get(Range=_range_string(pos, end))["Body"].read()
            logger.debug("get object from S3: size %d", len(self._buffer))
            with self._lock:
                self._buffer = buf
                self._buffer_pos = pos
                self._eof = (self._buffer_pos + len(self._buffer) == self._content_length)
            self._request.task_done()


class BufferedInputBase(io.BufferedIOBase):
    """Reads bytes from S3.

    Implements the io.BufferedIOBase interface of the standard library."""

    def __init__(self, bucket, key, buffer_size=DEFAULT_READ_BUFFER_SIZE, **kwargs):
        session = boto3.Session(profile_name=kwargs.pop('profile_name', None))
        s3 = session.resource('s3', **kwargs)
        self._object = s3.Object(bucket, key)
        self._raw_reader = RawReader(self._object)
        self._buffer_size = buffer_size
        self._content_length = self._object.content_length
        self._current_pos = 0
        self._buffer = b''
        self._buffer_pos = 0
        self._eof = False
        #
        # This member is part of the io.BufferedIOBase interface.
        #
        self.raw = None

    #
    # Override some methods from io.IOBase.
    #
    def close(self):
        """Flush and close this stream."""
        logger.debug("close: called")
        self._object = None

    def readable(self):
        """Return True if the stream can be read from."""
        return True

    def seekable(self):
        """If False, seek(), tell() and truncate() will raise IOError.

        We offer only seek support, and no truncate support."""
        return True

    def seek(self, offset, whence=START):
        """Seek to the specified position.

        :param int offset: The offset in bytes.
        :param int whence: Where the offset is from.

        Returns the position after seeking."""
        logger.debug('seeking to offset: %r whence: %r', offset, whence)
        if whence not in WHENCE_CHOICES:
            raise ValueError('invalid whence, expected one of %r' % WHENCE_CHOICES)

        if whence == START:
            new_position = offset
        elif whence == CURRENT:
            new_position = self._current_pos + offset
        else:
            new_position = self._content_length + offset
        new_position = _clamp(new_position, 0, self._content_length)

        logger.debug('new_position: %r', new_position)
        self._current_pos = new_position
        self._eof = self._current_pos == self._content_length
        if new_position < self._buffer_pos or \
           new_position >= self._buffer_pos + len(self._buffer):
            self._buffer = b""
            self._buffer_pos = new_position
        return self._current_pos

    def tell(self):
        """Return the current position within the file."""
        return self._current_pos

    def truncate(self, size=None):
        """Unsupported."""
        raise io.UnsupportedOperation

    #
    # io.BufferedIOBase methods.
    #
    def detach(self):
        """Unsupported."""
        raise io.UnsupportedOperation

    def read(self, size=-1):
        """Read up to size bytes from the object and return them."""
        if size <= 0:
            to_read = self.content_length - self._current_pos
        else:
            to_read = size

        parts = []
        while True:
            pos = self._current_pos - self._buffer_pos
            if pos + to_read <= len(self._buffer):
                parts.append(self._buffer[pos:(pos + to_read)])
                self._current_pos += to_read
                break
            elif self._buffer:
                parts.append(self._buffer[pos:])
                self._current_pos += len(parts[-1])
                to_read -= len(parts[-1])
            if self._eof:
                break
            self._buffer, self._buffer_pos, self._eof = self._raw_reader.read(
                self._buffer_pos + len(self._buffer),
                self._buffer_size)
        return b"".join(parts)

    def read1(self, size=-1):
        """This is the same as read()."""
        return self.read(size=size)

    def readline(self, limit=-1):
        """Read up to and including the next newline.  Returns the bytes read."""
        if limit != -1:
            raise NotImplementedError('limits other than -1 not implemented yet')
        parts = []
        while True:
            pos = self._current_pos - self._buffer_pos
            new_line_pos = self._buffer.find(b'\n', pos)
            if new_line_pos >= 0:
                parts.append(self._buffer[pos:new_line_pos+1])
                self._current_pos = self._buffer_pos + new_line_pos + 1
                break
            elif self._buffer:
                parts.append(self._buffer[pos:])
                self._current_pos = self._buffer_pos + len(self._buffer)
            if self._eof:
                break
            self._buffer, self._buffer_pos, self._eof = self._raw_reader.read(
                self._buffer_pos + len(self._buffer),
                self._buffer_size)
        return b"".join(parts)

    def readinto(self, b):
        """Read up to len(b) bytes into b, and return the number of bytes
        read."""
        data = self.read(len(b))
        if not data:
            return 0
        b[:len(data)] = data
        return len(data)

    def terminate(self):
        """Do nothing."""
        pass


class BufferedOutputBase(io.BufferedIOBase):
    """Writes bytes to S3.

    Implements the io.BufferedIOBase interface of the standard library."""

    def __init__(self, bucket, key, min_part_size=DEFAULT_MIN_PART_SIZE, **kwargs):
        if min_part_size < MIN_MIN_PART_SIZE:
            logger.warning("S3 requires minimum part size >= 5MB; \
multipart upload may fail")

        session = boto3.Session(profile_name=kwargs.pop('profile_name', None))
        s3 = session.resource('s3', **kwargs)

        #
        # https://stackoverflow.com/questions/26871884/how-can-i-easily-determine-if-a-boto-3-s3-bucket-resource-exists
        #
        s3.create_bucket(Bucket=bucket)
        self._object = s3.Object(bucket, key)
        self._min_part_size = min_part_size
        self._mp = self._object.initiate_multipart_upload()

        self._buf = io.BytesIO()
        self._total_bytes = 0
        self._total_parts = 0
        self._parts = []

        #
        # This member is part of the io.BufferedIOBase interface.
        #
        self.raw = None

    #
    # Override some methods from io.IOBase.
    #
    def close(self):
        logger.debug("closing")
        if self._buf.tell():
            self._upload_next_part()

        if self._total_bytes:
            self._mp.complete(MultipartUpload={'Parts': self._parts})
            logger.debug("completed multipart upload")
        elif self._mp:
            #
            # AWS complains with "The XML you provided was not well-formed or
            # did not validate against our published schema" when the input is
            # completely empty => abort the upload, no file created.
            #
            # We work around this by creating an empty file explicitly.
            #
            logger.info("empty input, ignoring multipart upload")
            assert self._mp, "no multipart upload in progress"
            self._mp.abort()

            self._object.put(Body=b'')
        self._mp = None
        logger.debug("successfully closed")

    @property
    def closed(self):
        return self._mp is None

    def writable(self):
        """Return True if the stream supports writing."""
        return True

    def tell(self):
        """Return the current stream position."""
        return self._total_bytes

    #
    # io.BufferedIOBase methods.
    #
    def detach(self):
        raise io.UnsupportedOperation("detach() not supported")

    def write(self, b):
        """Write the given bytes (binary string) to the S3 file.

        There's buffering happening under the covers, so this may not actually
        do any HTTP transfer right away."""
        if not isinstance(b, six.binary_type):
            raise TypeError("input must be a binary string, got: %r", b)

        # logger.debug("writing %r bytes to %r", len(b), self._buf)

        self._buf.write(b)
        self._total_bytes += len(b)

        if self._buf.tell() >= self._min_part_size:
            self._upload_next_part()

        return len(b)

    def terminate(self):
        """Cancel the underlying multipart upload."""
        assert self._mp, "no multipart upload in progress"
        self._mp.abort()
        self._mp = None

    #
    # Internal methods.
    #
    def _upload_next_part(self):
        part_num = self._total_parts + 1
        logger.info("uploading part #%i, %i bytes (total %.3fGB)",
                    part_num, self._buf.tell(), self._total_bytes / 1024.0 ** 3)
        self._buf.seek(0)
        part = self._mp.Part(part_num)
        upload = part.upload(Body=self._buf)
        self._parts.append({'ETag': upload['ETag'], 'PartNumber': part_num})
        logger.debug("upload of part #%i finished" % part_num)

        self._total_parts += 1
        self._buf = io.BytesIO()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.terminate()
        else:
            self.close()
