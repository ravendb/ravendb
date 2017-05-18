using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Lucene.Net.Search;
using Raven.Client.Exceptions;
using Sparrow;

namespace Raven.Server.ServerWide
{
    public unsafe class TempCryptoStream : Stream
    {
        private readonly string _file;
        private readonly FileStream _stream;

        public override bool CanRead => true;
        public override bool CanSeek => true;
        public override bool CanWrite => true;

        public override long Length
        {
            get
            {
                if (_blockNumber - 1 == _maxBlockWrittenToStream)
                    return (_blockNumber * _internalBuffer.Length) + _bufferValidIndex;
                return _stream.Length;
            }
        }

        public override long Position
        {
            get { return (_blockNumber * _internalBuffer.Length) + _bufferIndex; }
            set { Seek(value, SeekOrigin.Begin); }
        }

        private readonly byte[] _key;
        private readonly byte[] _nonce;
        private readonly byte[] _internalBuffer;
        private int _bufferIndex;
        private int _bufferValidIndex;
        private long _blockNumber;
        private long _maxBlockWrittenToStream = -1;


        public TempCryptoStream(string file)
        {
            _file = file;
            _stream = new FileStream(file, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None, 4096, FileOptions.DeleteOnClose | FileOptions.SequentialScan);
            _internalBuffer = new byte[4096];

            _key = new byte[Sodium.crypto_stream_xchacha20_keybytes()];
            _nonce = new byte[Sodium.crypto_stream_xchacha20_noncebytes()];

            fixed (byte* pKey = _key)
            fixed (byte* pNonce = _nonce)
            {
                Sodium.randombytes_buf(pKey, (UIntPtr)Sodium.crypto_stream_xchacha20_keybytes());
                Sodium.randombytes_buf(pNonce, (UIntPtr)Sodium.crypto_stream_xchacha20_noncebytes());
            }
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (offset < 0 || offset >= buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(offset));
            if (count < 0 || offset + count > buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(count));

            fixed (byte* pInternalBuffer = _internalBuffer)
            fixed (byte* pInputBuffer = buffer)
            {
                // Write to the internal buffer. Only when it is full, encrypt the entire buffer (or "block") and write it to the inner stream
                while (count + _bufferIndex >= _internalBuffer.Length)
                {
                    var bytesToCopy = _internalBuffer.Length - _bufferIndex;
                    Sparrow.Memory.Copy(pInternalBuffer + _bufferIndex, pInputBuffer + offset, bytesToCopy);
                    count -= bytesToCopy;
                    offset += bytesToCopy;
                    _bufferIndex += bytesToCopy;
                    _bufferValidIndex = Math.Max(_bufferValidIndex, _bufferIndex);

                    EncryptToStream(pInternalBuffer);

                    if (_blockNumber > _maxBlockWrittenToStream)
                    {
                        _maxBlockWrittenToStream = _blockNumber;
                        _blockNumber++;
                    }
                    else
                    {
                        Seek((_blockNumber+1)*_internalBuffer.Length, SeekOrigin.Begin);
                    }
                }

                // small write or the remains of a big one
                if (count > 0) 
                {
                    Sparrow.Memory.Copy(pInternalBuffer + _bufferIndex, pInputBuffer + offset, count);
                    _bufferIndex += count;
                    _bufferValidIndex = Math.Max(_bufferValidIndex, _bufferIndex);
                }
            }
        }


        public override void Flush()
        {
            // intentionally not flushing to the inner stream, no need to force a temp file to go to disk
        }

        private void EncryptToStream(byte* pInternalBuffer)
        {
            if (_bufferValidIndex == 0)
                return;

            fixed (byte* n = _nonce)
            fixed (byte* k = _key)
            {
                _stream.Seek(_blockNumber * _internalBuffer.Length, SeekOrigin.Begin);

                var rc = Sodium.crypto_stream_xchacha20_xor_ic(pInternalBuffer, pInternalBuffer, (ulong)_bufferValidIndex, n, (ulong)_blockNumber, k);
                if (rc != 0)
                    throw new InvalidOperationException($"crypto_stream_xchacha20_xor failed in EncryptToStream(). rc={rc}, _bufferIndex={_bufferIndex}, _blockNumber={_blockNumber}");

                _stream.Write(_internalBuffer, 0, _bufferValidIndex);
                _bufferIndex = 0;
                _bufferValidIndex = 0;
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (offset < 0 || offset >= buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(offset));
            if (count < 0 || offset + count > buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(count));

            fixed (byte* pInternalBuffer = _internalBuffer)
            fixed (byte* pInputBuffer = buffer)
            fixed (byte* k = _key)
            fixed (byte* n = _nonce)
            {
                if (_bufferIndex >= _bufferValidIndex)
                {
                    _bufferIndex = 0;
                    var totalRead = 0;

                    while (totalRead < _internalBuffer.Length)
                    {
                        var currentRead = _stream.Read(_internalBuffer, totalRead, _internalBuffer.Length - totalRead);
                        if (currentRead == 0)
                        {
                            if (totalRead == 0)
                                return 0; // done
                            break; // still have some stuff in the buffer to return
                        }

                        totalRead += currentRead;
                    }

                    _bufferValidIndex = totalRead;

                    var rc = Sodium.crypto_stream_xchacha20_xor_ic(pInternalBuffer, pInternalBuffer, (ulong)_bufferValidIndex, n, (ulong)_blockNumber, k);
                    if (rc != 0)
                        throw new InvalidOperationException($"crypto_stream_xchacha20_xor failed during Read(). rc={rc}, _bufferValidIndex={_bufferValidIndex}, _blockNumber={_blockNumber}");
                }

                var toRead = Math.Min(count, _bufferValidIndex - _bufferIndex);
                Sparrow.Memory.Copy(pInputBuffer + offset, pInternalBuffer + _bufferIndex, toRead);
                _bufferIndex += toRead;

                if (_bufferValidIndex == _internalBuffer.Length && _bufferValidIndex == _bufferIndex)
                    _blockNumber++;

                return toRead;
            }
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            if (offset < 0 || origin != SeekOrigin.Begin)
                throw new NotSupportedException();

            var blockNumber = offset / _internalBuffer.Length;
            var positionInsideBlock = (int)offset % _internalBuffer.Length;
            if (blockNumber == _blockNumber && positionInsideBlock < _bufferValidIndex)
            {
                _bufferIndex = positionInsideBlock;
                return offset;
            }

            // First, let's write what we already have in the internal buffer.
            fixed (byte* pInternalBuffer = _internalBuffer)
            {
                EncryptToStream(pInternalBuffer);
            }

            // Seek to start of requested block and read into the internal buffer
            _stream.Seek(offset - positionInsideBlock, SeekOrigin.Begin); 

            fixed (byte* pInternalBuffer = _internalBuffer)
            fixed (byte* k = _key)
            fixed (byte* n = _nonce)
            {
                var count = _internalBuffer.Length;
                _bufferValidIndex = 0;
                while (count > 0)
                {
                    var read = _stream.Read(_internalBuffer, _bufferValidIndex, count);
                    if (read == 0)
                        break;
                    count -= read;
                    _bufferValidIndex += read;
                }

                var rc = Sodium.crypto_stream_xchacha20_xor_ic(pInternalBuffer, pInternalBuffer, (ulong)_bufferValidIndex, n, (ulong)blockNumber, k);
                if (rc != 0)
                    throw new InvalidOperationException($"crypto_stream_xchacha20_xor failed during Seek(). rc={rc}, _bufferValidIndex={_bufferValidIndex}, blockNumber={blockNumber}");
                _blockNumber = blockNumber;
                _bufferIndex = positionInsideBlock;
            }
            return offset;
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException("Cannot set length in TempCryptoStream");
        }

        protected override void Dispose(bool disposing)
        {
            _stream.Dispose();
            try
            {
                if (File.Exists(_file)) // On Linux we don't get DeleteOnClose
                    File.Delete(_file);
            }
            catch (Exception)
            {
                // ignore, nothing we can do here
            }
        }
    }
}
