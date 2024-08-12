using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.Util
{
    internal sealed class StreamWithTimeout(Stream stream) : Stream
    {
        internal static TimeSpan DefaultReadTimeout { get; } = TimeSpan.FromSeconds(120);

        internal readonly Stream _stream = stream;
        private int _writeTimeout;
        private int _readTimeout;
        private CancellationTokenSource _writeCts;
        private CancellationTokenSource _readCts;

        private long _totalRead = 0;
        private long _totalWritten = 0;

        public override int ReadTimeout
        {
            get => _readTimeout;
            set
            {
                if (_stream.CanRead && _stream.CanTimeout)
                {
                    _stream.ReadTimeout = value; // we only need to set it when base stream supports that, if not we are handling that ourselves
                    return;
                }
                _readTimeout = value;
            }
        }

        public override int WriteTimeout
        {
            get => _writeTimeout;
            set
            {
                if (_stream.CanWrite && _stream.CanTimeout)
                {
                    _stream.WriteTimeout = value;  // we only need to set it when base stream supports that, if not we are handling that ourselves
                    return;
                }

                _writeTimeout = value;
            }
        }

        public override bool CanTimeout => true;

        public override void Flush()
        {
            _stream.Flush();
        }

        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            return _stream.FlushAsync(cancellationToken);
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (_readTimeout == 0)
            {
                var read = _stream.Read(buffer, offset, count);
                _totalRead += read;
                return read;
            }

            // _totalRead is counted in ReadAsyncWithTimeout
            return AsyncHelpers.RunSync(() => ReadAsyncWithTimeout(buffer, offset, count, CancellationToken.None));
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            // _totalRead is counted in ReadAsyncWithTimeout
            return ReadAsyncWithTimeout(buffer, offset, count, cancellationToken);
        }

        private async Task<int> ReadAsyncWithTimeout(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            var ct = cancellationToken;
            if (_readTimeout > 0)
            {
                _readCts?.Dispose();
                _readCts = cancellationToken == default ? new CancellationTokenSource() : CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                _readCts.CancelAfter(_readTimeout);
                ct = _readCts.Token;
            }

            var read = await _stream.ReadAsync(buffer, offset, count, ct).ConfigureAwait(false);
            _totalRead += read;
            return read;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            return _stream.Seek(offset, origin);
        }

        public override void SetLength(long value)
        {
            _stream.SetLength(value);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            _totalWritten += count;

            if (_writeTimeout == 0)
            {
                _stream.Write(buffer, offset, count);
                return;
            }

            AsyncHelpers.RunSync(() => WriteAsyncWithTimeout(buffer, offset, count, CancellationToken.None));
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            _totalWritten += count;

            return WriteAsyncWithTimeout(buffer, offset, count, cancellationToken);
        }

        private Task WriteAsyncWithTimeout(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            var ct = cancellationToken;
            if (_writeTimeout > 0)
            {
                _writeCts?.Dispose();
                _writeCts = cancellationToken == default ? new CancellationTokenSource() : CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                _writeCts.CancelAfter(_writeTimeout);
                ct = _writeCts.Token;
            }

            return _stream.WriteAsync(buffer, offset, count, ct);
        }

        public override bool CanRead => _stream.CanRead;
        public override bool CanSeek => _stream.CanSeek;
        public override bool CanWrite => _stream.CanWrite;
        public override long Length => _stream.Length;

        public long TotalRead => _totalRead;
        public long TotalWritten => _totalWritten;

        public override long Position
        {
            get => _stream.Position;
            set => _stream.Position = value;
        }

        ~StreamWithTimeout()
        {
            _readCts?.Dispose();
            _writeCts?.Dispose();
        }

        protected override void Dispose(bool disposing)
        {
            GC.SuppressFinalize(this);

            _stream.Dispose();
            base.Dispose(disposing);

            _readCts?.Dispose();
            _writeCts?.Dispose();
        }

#if NETSTANDARD2_0
        public ValueTask DisposeAsync()
#else
        public override async ValueTask DisposeAsync()
#endif
        {
#if NETSTANDARD2_0
            Dispose();
            return new ValueTask();
#else
            GC.SuppressFinalize(this);

            await _stream.DisposeAsync().ConfigureAwait(false);
            await base.DisposeAsync().ConfigureAwait(false);

            _readCts?.Dispose();
            _writeCts?.Dispose();
#endif
        }
    }
}
