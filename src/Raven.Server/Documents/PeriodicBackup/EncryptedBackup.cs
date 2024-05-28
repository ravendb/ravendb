using System;
using System.Diagnostics;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Util;
using Sparrow.Platform;
using Sparrow.Server.Global;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class DecryptingXChaCha20Oly1305Stream : Stream
    {
        private readonly Stream _inner;
        private readonly byte[] _key;
        private readonly byte[] _pullState;
        private readonly byte[] _encryptedBuffer = new byte[Constants.Encryption.DefaultBufferSize + Constants.Encryption.XChachaAdLen];
        private readonly byte[] _plainTextBuffer = new byte[Constants.Encryption.DefaultBufferSize];
        private Memory<byte> _plainTextWindow = Memory<byte>.Empty;
        private bool _initialized;

        public DecryptingXChaCha20Oly1305Stream(Stream inner, byte[] key)
        {
            if (key.Length != (int)Sodium.crypto_secretstream_xchacha20poly1305_keybytes())
                throw new InvalidOperationException($"The size of the key must be " +
                                                    $"{(int)Sodium.crypto_secretstream_xchacha20poly1305_keybytes()} bytes, " +
                                                    $"but was {key.Length} bytes.");

            _inner = inner;
            _key = key;
            _pullState = new byte[(int)Sodium.crypto_secretstream_xchacha20poly1305_statebytes()];
        }

        public async ValueTask InitializeAsync()
        {
            var headerbytes = (int)Sodium.crypto_secretstream_xchacha20poly1305_headerbytes();

            byte[] header = new byte[headerbytes];

            var readingHeader = new Memory<byte>(header);
            while (readingHeader.IsEmpty == false)
            {
                var read = await _inner.ReadAsync(readingHeader);
                if (read == 0)
                    throw new EndOfStreamException("Wrong or corrupted file, we are missing the header");

                readingHeader = readingHeader.Slice(read);
            }

            unsafe
            {
                fixed (byte* ps = _pullState, pKey = _key, pHeader = header)
                {
                    if (Sodium.crypto_secretstream_xchacha20poly1305_init_pull(ps, pHeader, pKey) != 0)
                        throw new CryptographicException("Failed to init state, wrong key or corrupted file");
                }
            }

            _initialized = true;
        }


        public override void Flush()
        {
            throw new NotSupportedException();
        }

        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            EnsureInitialized();

            if (TryReadFromBuffer(buffer, offset, count, out int read))
                return read;

            while (read < _encryptedBuffer.Length)
            {
                int rc = _inner.Read(_encryptedBuffer, read, _encryptedBuffer.Length - read);
                if (rc == 0)
                    break;
                read += rc;
            }

            if (read == 0)
                return 0;

            DecryptBuffer(read);

            return Read(buffer, offset, count);
        }

        private unsafe void DecryptBuffer(int read)
        {
            fixed (byte* ciphertextPtr = _encryptedBuffer, statePtr = _pullState, plain = _plainTextBuffer)
            {
                ulong s;
                if (Sodium.crypto_secretstream_xchacha20poly1305_pull(statePtr, plain, &s, null, ciphertextPtr, (ulong)read, null, 0) != 0)
                    throw new CryptographicException("Failed to decryped file. Wrong or corrupted file or key");
                _plainTextWindow = new Memory<byte>(_plainTextBuffer, 0, (int)s);
            }
        }

        private unsafe bool TryReadFromBuffer(byte[] buffer, int offset, int count, out int read)
        {
            if (_plainTextWindow.IsEmpty == false)
            {
                var sizeToCopy = Math.Min(count, _plainTextWindow.Length);
                fixed (byte* src = _plainTextWindow.Span, dst = buffer)
                {
                    Buffer.MemoryCopy(src, dst + offset, count, sizeToCopy);
                    _plainTextWindow = _plainTextWindow.Slice(sizeToCopy);
                    read = sizeToCopy;
                    return true;
                }
            }

            read = 0;
            return false;
        }

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            EnsureInitialized();

            if (TryReadFromBuffer(buffer, offset, count, out int read))
                return read;

            read = 0;
            while (read < _encryptedBuffer.Length)
            {
                int rc = await _inner.ReadAsync(_encryptedBuffer, read, _encryptedBuffer.Length - read, cancellationToken);
                if (rc == 0)
                    break;
                read += rc;
            }

            if (read == 0)
                return 0;

            DecryptBuffer(read);

            return await ReadAsync(buffer, offset, count, cancellationToken);
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        [Conditional("DEBUG")]
        public void EnsureInitialized()
        {
            if (_initialized == false)
                throw new InvalidOperationException($"{nameof(DecryptingXChaCha20Oly1305Stream)} must be initialized");
        }

        protected override void Dispose(bool disposing)
        {
            _inner.Dispose();
            base.Dispose(disposing);
        }
    }

    public class EncryptingXChaCha20Poly1305Stream : Stream
    {
        private readonly Stream _inner;
        private readonly byte[] _key;
        private readonly byte[] _pushState;
        private readonly byte[] _encryptedBuffer = new byte[Constants.Encryption.DefaultBufferSize + Constants.Encryption.XChachaAdLen];
        private readonly byte[] _innerBuffer = new byte[Constants.Encryption.DefaultBufferSize];
        private int _pos = 0;
        private bool _shouldFlush;
        private bool _initialized;

        public EncryptingXChaCha20Poly1305Stream(Stream inner, byte[] key)
        {
            if (key.Length != (int)Sodium.crypto_secretstream_xchacha20poly1305_keybytes())
                throw new InvalidOperationException($"The size of the key must be " +
                                                    $"{(int)Sodium.crypto_secretstream_xchacha20poly1305_keybytes()} bytes, " +
                                                    $"but was {key.Length} bytes.");

            _inner = inner;
            _key = key;
            _pushState = new byte[(int)Sodium.crypto_secretstream_xchacha20poly1305_statebytes()];
        }

        public async Task InitializeAsync()
        {
            var headerbytes = (int)Sodium.crypto_secretstream_xchacha20poly1305_headerbytes();
            var header = new byte[headerbytes];

            unsafe
            {
                fixed (byte* ps = _pushState, pKey = _key, pHeader = header)
                {
                    if (Sodium.crypto_secretstream_xchacha20poly1305_init_push(ps, pHeader, pKey) != 0)
                        throw new CryptographicException("Failed to init state, wrong or corrupted key");
                }
            }

            await _inner.WriteAsync(new Memory<byte>(header));

            _initialized = true;
        }

        public void Initialize()
        {
            AsyncHelpers.RunSync(InitializeAsync);
        }

        public override void Flush()
        {
            EnsureInitialized();

            // no-op - because we *cannot* do anything here, we *require* that all writes, except the last one
            // will be on 4Kb aligned value. This is already handled, but we may want to flush past writes, so
            // will allow it
            if (_shouldFlush)
            {
                _inner.Flush();
                _shouldFlush = false;
            }
        }

        public override async Task FlushAsync(CancellationToken cancellationToken)
        {
            EnsureInitialized();
            
            // no-op - because we *cannot* do anything here, we *require* that all writes, except the last one
            // will be on 4Kb aligned value. This is already handled, but we may want to flush past writes, so
            // will allow it
            if (_shouldFlush)
            {
                await _inner.FlushAsync(cancellationToken);
                _shouldFlush = false;
            }
        }

        public void Flush(bool flushToDisk)
        {
            EnsureInitialized();

            // no-op - because we *cannot* do anything here, we *require* that all writes, except the last one
            // will be on 4Kb aligned value. This is already handled, but we may want to flush past writes, so
            // will allow it
            if (_shouldFlush)
            {
                if (_inner is FileStream innerFS)
                    innerFS.Flush(flushToDisk: flushToDisk);
                else
                    _inner.Flush();

                _shouldFlush = false;
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        private unsafe int EncryptBuffer()
        {
            fixed (byte* fileBlockPtr = _innerBuffer, ciphertextPtr = _encryptedBuffer, statePtr = _pushState)
            {
                ulong size;
                if (Sodium.crypto_secretstream_xchacha20poly1305_push(statePtr, ciphertextPtr, &size, fileBlockPtr, (ulong)_pos, null, 0, 0) != 0)
                    throw new CryptographicException("Failed to encrypt backup");

                return (int)size;
            }
        }

        private void FlushAndEncryptBuffer()
        {
            if (_pos == 0)
                return;
            var sizeToWrite = EncryptBuffer();
            _inner.Write(_encryptedBuffer, 0, sizeToWrite);
            _pos = 0;
            _shouldFlush = true;
        }

        private async Task FlushAndEncryptBufferAsync()
        {
            if (_pos == 0)
                return;
            var sizeToWrite = EncryptBuffer();
            await _inner.WriteAsync(_encryptedBuffer, 0, sizeToWrite);
            _pos = 0;
            _shouldFlush = true;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            EnsureInitialized();

            while (count > 0)
            {
                var sizeToWrite = Math.Min(Constants.Encryption.DefaultBufferSize - _pos, count);
                Buffer.BlockCopy(buffer, offset, _innerBuffer, _pos, sizeToWrite);
                count -= sizeToWrite;
                offset += sizeToWrite;
                _pos += sizeToWrite;
                if (_pos == Constants.Encryption.DefaultBufferSize)
                {
                    FlushAndEncryptBuffer();
                }
            }
        }

        public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            EnsureInitialized();

            while (count > 0)
            {
                var sizeToWrite = Math.Min(Constants.Encryption.DefaultBufferSize - _pos, count);
                Buffer.BlockCopy(buffer, offset, _innerBuffer, _pos, sizeToWrite);
                count -= sizeToWrite;
                offset += sizeToWrite;
                _pos += sizeToWrite;
                if (_pos == Constants.Encryption.DefaultBufferSize)
                {
                    await FlushAndEncryptBufferAsync();
                }
            }
        }

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        [Conditional("DEBUG")]
        public void EnsureInitialized()
        {
            if (_initialized == false)
                throw new InvalidOperationException($"{nameof(EncryptingXChaCha20Poly1305Stream)} must be initialized");
        }

        protected override void Dispose(bool disposing)
        {
            GC.SuppressFinalize(this);
            FlushAndEncryptBuffer();
            Flush(flushToDisk: true);
            _inner.Dispose();
        }

        public override async ValueTask DisposeAsync()
        {
            GC.SuppressFinalize(this);
            await FlushAndEncryptBufferAsync();
            await FlushAsync();
            await _inner.DisposeAsync();
        }
    }
}
