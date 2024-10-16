using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.Util
{
    internal sealed class StreamWithTimeout : Stream
    {
        internal static TimeSpan DefaultReadTimeout { get; } = TimeSpan.FromSeconds(120);

        private Stopwatch _writeSw;
        private Stopwatch _readSw;

        internal readonly Stream _stream;
        private int _writeTimeout;
        private int _readTimeout;
        private int _minimumWriteDelayTimeInMs;
        private int _minimumReadDelayTimeInMs;
        private CancellationTokenSource _writeCts;
        private CancellationTokenSource _readCts;

#if DEBUG
        private CancellationToken _requestReadCts;
        private CancellationToken _requestWriteCts;
#endif

        private long _totalRead;
        private long _totalWritten;

        public StreamWithTimeout(Stream stream)
        {
            _stream = stream;
        }

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
                _minimumReadDelayTimeInMs = value / 3;
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
                _minimumWriteDelayTimeInMs = value / 3;
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
            if (_readTimeout > 0)
            {
                if (_readCts == null)
                {
                _readCts = GenerateCancellationTokenWithTimeout(_readTimeout, cancellationToken);
                    _readSw = Stopwatch.StartNew();

#if DEBUG
                    _requestReadCts = cancellationToken;
#endif
                }
                else if (_readSw.ElapsedMilliseconds > _minimumReadDelayTimeInMs)
                {
                    _readSw.Restart();
                    _readCts.CancelAfter(_readTimeout);

                if (_readCts.IsCancellationRequested)
                {
                    _readCts?.Dispose();
                    _readCts = GenerateCancellationTokenWithTimeout(_readTimeout, cancellationToken);
                }

#if DEBUG
                    if (_requestReadCts != cancellationToken)
                        throw new InvalidOperationException("The cancellation token was changed during the request");
#endif
                }

                cancellationToken = _readCts.Token;
            }

            var read = await _stream.ReadAsync(buffer, offset, count, cancellationToken).ConfigureAwait(false);
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
            if (_writeTimeout > 0)
            {
                if (_writeCts == null)
                {
                _writeCts = GenerateCancellationTokenWithTimeout(_writeTimeout, cancellationToken);
                    _writeSw = Stopwatch.StartNew();

#if DEBUG
                    _requestWriteCts = cancellationToken;
#endif
                }
                else if (_writeSw.ElapsedMilliseconds > _minimumWriteDelayTimeInMs)
                {
                    _writeSw.Restart();
                    _writeCts.CancelAfter(_writeTimeout);

                if (_writeCts.IsCancellationRequested)
                {
                    _writeCts.Dispose();
                    _writeCts = GenerateCancellationTokenWithTimeout(_writeTimeout, cancellationToken);
                }

#if DEBUG
                    if (_requestWriteCts != cancellationToken)
                        throw new InvalidOperationException("The cancellation token was changed during the request");
#endif
                }

                cancellationToken = _writeCts.Token;
            }

            return _stream.WriteAsync(buffer, offset, count, cancellationToken);
        }

        private static CancellationTokenSource GenerateCancellationTokenWithTimeout(int timeout, CancellationToken ct)
        {
            var cts = ct == default ? new CancellationTokenSource() : CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(timeout);
            return cts;
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
