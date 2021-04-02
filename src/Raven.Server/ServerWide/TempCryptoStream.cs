using System;
using System.Diagnostics;
using System.IO;
using Raven.Server.Utils;
using Sparrow.Server;
using Sparrow.Utils;

namespace Raven.Server.ServerWide
{
    public unsafe class TempCryptoStream : Stream
    {
        private readonly string _file;
        private bool _ignoreSetLength;
        private readonly FileStream _stream;
        private readonly MemoryStream _authenticationTags = new MemoryStream();
        private readonly MemoryStream _nonces = new MemoryStream();
        private readonly long _startPosition;

        public FileStream InnerStream => _stream; 
        public override bool CanRead => true;
        public override bool CanSeek => true;
        public override bool CanWrite => true;

        public override long Length
        {
            get
            {
                return _maxLength;
            }
        }

        public override long Position
        {
            get => _blockNumber * _internalBuffer.Length + _bufferIndex;
            set => Seek(value, SeekOrigin.Begin);
        }

        private readonly byte[] _key;
        private readonly byte[] _internalBuffer;    // Temp buffer for one block only
        private int _bufferIndex;    // The position in the block buffer.
        private int _bufferValidIndex;
        private bool _needToWrite = false;
        private long _blockNumber;
        private long _maxLength;

        public TempCryptoStream(string file) : this(SafeFileStream.Create(file, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None, 4096, FileOptions.DeleteOnClose))
        {
            _file = file;
        }

        public TempCryptoStream IgnoreSetLength()
        {
            _ignoreSetLength = true;
            return this;
        }

        public TempCryptoStream(FileStream stream)
        {
            _stream = stream;
            _startPosition = stream.Position;
            _internalBuffer = new byte[4096];

            _key = new byte[(int)Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes()];

            fixed (byte* pKey = _key)
            {
                Sodium.randombytes_buf(pKey, Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes());
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
                    _needToWrite = true;
                    count -= bytesToCopy;
                    offset += bytesToCopy;
                    _bufferIndex += bytesToCopy;
                    _bufferValidIndex = Math.Max(_bufferValidIndex, _bufferIndex);

                    EncryptToStream(pInternalBuffer);

                    var positionOfWrite = (_blockNumber +1) * _internalBuffer.Length + _bufferIndex;

                    if (positionOfWrite >= _maxLength)
                    {
                        _blockNumber++;
                    }
                    else
                    {
                        Seek((_blockNumber + 1) * _internalBuffer.Length, SeekOrigin.Begin);
                    }
                }

                // small write or the remains of a big one
                if (count > 0)
                {
                    _needToWrite = true;
                    Sparrow.Memory.Copy(pInternalBuffer + _bufferIndex, pInputBuffer + offset, count);
                    _bufferIndex += count;
                    _maxLength = Math.Max(_maxLength,  _blockNumber * _internalBuffer.Length + _bufferIndex);
                    _bufferValidIndex = Math.Max(_bufferValidIndex, _bufferIndex);
                }
            }
        }


        public override void Flush()
        {
            // First, let's write what we already have in the internal buffer.
            fixed (byte* pInternalBuffer = _internalBuffer)
            {
                EncryptToStream(pInternalBuffer);
            }
            // intentionally not flushing to the inner stream, no need to force a temp file to go to disk
        }

        private void EncryptToStream(byte* pInternalBuffer)
        {
            if (_bufferValidIndex == 0  || _needToWrite == false)
                return;
            
            if (_bufferValidIndex != _internalBuffer.Length)
            {
                if (_stream.Length != _stream.Position)
                {
                    throw new InvalidOperationException("Writing less than a block size is only allowed at the end of the stream");
                }
            }

            fixed (byte* k = _key)
            {
                _stream.Seek(_startPosition + _blockNumber * _internalBuffer.Length, SeekOrigin.Begin);
                EnsureDetachedStreamsLength();

                fixed (byte* pAuthTags = _authenticationTags.GetBuffer())
                fixed (byte* pNonces = _nonces.GetBuffer())
                {
                    var mac = pAuthTags + _blockNumber* (int)Sodium.crypto_aead_xchacha20poly1305_ietf_abytes();
                    var nonce = pNonces + _blockNumber * (int)Sodium.crypto_aead_xchacha20poly1305_ietf_npubbytes();
                    Sodium.randombytes_buf(nonce, Sodium.crypto_aead_xchacha20poly1305_ietf_npubbytes());
                    ulong macLen = 0;
                    var rc = Sodium.crypto_aead_xchacha20poly1305_ietf_encrypt_detached(pInternalBuffer, mac, &macLen , pInternalBuffer, (ulong)_bufferValidIndex, null,0, null, nonce, k);
                    if (rc != 0)
                        throw new InvalidOperationException(
                            $"crypto_stream_xchacha20_xor failed in EncryptToStream(). rc={rc}, _bufferIndex={_bufferIndex}, _blockNumber={_blockNumber}");
                    Debug.Assert(macLen == (ulong)Sodium.crypto_aead_xchacha20poly1305_ietf_abytes());
                }

                _stream.Write(_internalBuffer, 0, _bufferValidIndex);
                _maxLength = Math.Max(_stream.Position - _startPosition, _maxLength);
                _bufferIndex = 0;
                _bufferValidIndex = 0;
                _needToWrite = false;
            }
        }

        private void EnsureDetachedStreamsLength()
        {
            var requiredLength = _blockNumber * (int)Sodium.crypto_aead_xchacha20poly1305_ietf_abytes() + (int)Sodium.crypto_aead_xchacha20poly1305_ietf_abytes();
            if (_authenticationTags.Length < requiredLength)
            {
                _authenticationTags.SetLength(requiredLength);
            }
            
            requiredLength = _blockNumber * (int)Sodium.crypto_aead_xchacha20poly1305_ietf_npubbytes() + (int)Sodium.crypto_aead_xchacha20poly1305_ietf_npubbytes();
            if (_nonces.Length < requiredLength)
            {
                _nonces.SetLength(requiredLength);
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (offset < 0 || offset >= buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(offset));
            if (count < 0 || offset + count > buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(count));

            if (_bufferIndex >= _bufferValidIndex)
            {
                _blockNumber++;
                if (ReadIntoBuffer() == 0)
                    return 0;
            }
            
            var toRead = Math.Min(count, _bufferValidIndex - _bufferIndex);

            Buffer.BlockCopy(_internalBuffer, _bufferIndex, buffer, offset, toRead);
            _bufferIndex += toRead;
            if (_bufferValidIndex == _internalBuffer.Length && _bufferValidIndex == _bufferIndex + toRead)
                _blockNumber++;

            return toRead;
        }

        private int ReadIntoBuffer()
        {
            _bufferIndex = 0;
            var totalRead = 0;

            var positionOfRead = _blockNumber * _internalBuffer.Length + _bufferIndex;
            while (totalRead < _internalBuffer.Length)
            {
                var amountToRead = Math.Min(_internalBuffer.Length - totalRead, Math.Max(0, _maxLength - positionOfRead));
                var currentRead = _stream.Read(_internalBuffer, totalRead, (int)amountToRead);
                positionOfRead += currentRead;
                if (currentRead == 0)
                {
                    if (totalRead == 0)
                        return 0; // done
                    break; // still have some stuff in the buffer to return
                }

                totalRead += currentRead;
            }

            _bufferValidIndex = totalRead;

            EnsureDetachedStreamsLength();

            fixed (byte* pInternalBuffer = _internalBuffer)
            fixed (byte* k = _key)
            fixed (byte* pAuthTags = _authenticationTags.GetBuffer())
            fixed (byte* pNonces = _nonces.GetBuffer())
            {
                var mac = pAuthTags + _blockNumber * (int)Sodium.crypto_aead_xchacha20poly1305_ietf_abytes();
                var nonce = pNonces + _blockNumber * (int)Sodium.crypto_aead_xchacha20poly1305_ietf_npubbytes();


                var rc = Sodium.crypto_aead_xchacha20poly1305_ietf_decrypt_detached(pInternalBuffer, null, pInternalBuffer, (ulong)_bufferValidIndex, mac, null, 0, nonce, k);
                if (rc != 0)
                    throw new InvalidOperationException(
                        $"crypto_stream_xchacha20_xor failed during Read(). rc={rc}, _bufferValidIndex={_bufferValidIndex}, _blockNumber={_blockNumber}");
            }

            return _bufferValidIndex;
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
            var pos = _stream.Seek(_startPosition + offset - positionInsideBlock, SeekOrigin.Begin);
            _blockNumber = blockNumber;
            ReadIntoBuffer();
            _bufferIndex = positionInsideBlock;

            return pos;
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException("Cannot set length in TempCryptoStream");
        }

        protected override void Dispose(bool disposing)
        {
            if (_file != null)
            {
                _stream.Dispose();
                PosixFile.DeleteOnClose(_file);
            }
        }
    }
}
