﻿using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.Util
{
    internal class StreamWithTimeout : Stream, IAsyncDisposable
    {
        private static readonly TimeSpan DefaultWriteTimeout = TimeSpan.FromSeconds(120);
        internal static TimeSpan DefaultReadTimeout { get; } = TimeSpan.FromSeconds(120);

        private static readonly int DefaultMinimumWriteDelayTimeInMs = (int)(DefaultWriteTimeout.TotalMilliseconds / 3);
        private static readonly int DefaultMinimumReadDelayTimeInMs = (int)(DefaultReadTimeout.TotalMilliseconds / 3);

        private Stopwatch _writeSw;
        private Stopwatch _readSw;

        private readonly Stream _stream;
        private int _writeTimeout;
        private int _readTimeout;
        private int _minimumWriteDelayTimeInMs = DefaultMinimumWriteDelayTimeInMs;
        private int _minimumReadDelayTimeInMs = DefaultMinimumReadDelayTimeInMs;
        private bool _canBaseStreamTimeoutOnWrite;
        private bool _canBaseStreamTimeoutOnRead;
        private CancellationTokenSource _writeCts;
        private CancellationTokenSource _readCts;

#if DEBUG
        private CancellationToken _requestReadCts;
        private CancellationToken _requestWriteCts;
#endif

        private long _totalRead = 0;
        private long _totalWritten = 0;

        public StreamWithTimeout(Stream stream)
        {
            _stream = stream;
            SetWriteTimeoutIfNeeded(DefaultWriteTimeout);
            SetReadTimeoutIfNeeded(DefaultReadTimeout);
        }

        private void SetReadTimeoutIfNeeded(TimeSpan readTimeout)
        {
            try
            {
                _readTimeout = (int)readTimeout.TotalMilliseconds;
                _canBaseStreamTimeoutOnRead = _stream.CanRead && _stream.CanTimeout;

                if (_canBaseStreamTimeoutOnRead)
                {
                    var streamReadTimeout = _stream.ReadTimeout;
                    if (streamReadTimeout > 0)
                    {
                        ReadTimeout = streamReadTimeout;
                        return;
                    }

                    try
                    {
                        _stream.ReadTimeout = _readTimeout;
                    }
                    catch
                    {
                        if (streamReadTimeout <= _readTimeout)
                            ReadTimeout = streamReadTimeout;
                        else
                            _canBaseStreamTimeoutOnRead = false;
                    }
                }
            }
            catch
            {
                _canBaseStreamTimeoutOnRead = false;
            }
        }

        private void SetWriteTimeoutIfNeeded(TimeSpan writeTimeout)
        {
            try
            {
                _writeTimeout = (int)writeTimeout.TotalMilliseconds;
                _canBaseStreamTimeoutOnWrite = _stream.CanWrite && _stream.CanTimeout;

                if (_canBaseStreamTimeoutOnWrite)
                {
                    var streamWriteTimeout = _stream.WriteTimeout;
                    if (streamWriteTimeout > 0)
                    {
                        WriteTimeout = streamWriteTimeout;
                        return;
                    }
                    try
                    {
                        _stream.WriteTimeout = _writeTimeout;
                    }
                    catch
                    {
                        if (streamWriteTimeout <= _writeTimeout)
                            WriteTimeout = streamWriteTimeout;
                        else
                            _canBaseStreamTimeoutOnWrite = false;
                    }
                }
            }
            catch
            {
                _canBaseStreamTimeoutOnWrite = false;
            }
        }

        public override int ReadTimeout
        {
            get => _readTimeout;
            set
            {
                if (_canBaseStreamTimeoutOnRead)
                    _stream.ReadTimeout = value; // we only need to set it when base stream supports that, if not we are handling that ourselves

                _readTimeout = value;
                _minimumReadDelayTimeInMs = value / 3;
            }
        }

        public override int WriteTimeout
        {
            get => _writeTimeout;
            set
            {
                if (_canBaseStreamTimeoutOnWrite)
                    _stream.WriteTimeout = value;  // we only need to set it when base stream supports that, if not we are handling that ourselves

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
            if (_canBaseStreamTimeoutOnRead)
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

            var read = await _stream.ReadAsync(buffer, offset, count, _readCts.Token).ConfigureAwait(false);
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

            if (_canBaseStreamTimeoutOnWrite)
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

            return _stream.WriteAsync(buffer, offset, count, _writeCts.Token);
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
            base.Dispose(disposing);
            _stream.Dispose();
            _readCts?.Dispose();
            _writeCts?.Dispose();
        }

#if NETSTANDARD2_0 || NETCOREAPP2_1
        public ValueTask DisposeAsync()
#else
        public override async ValueTask DisposeAsync()
#endif
        {
#if NETSTANDARD2_0 || NETCOREAPP2_1
            Dispose();
            return new ValueTask();
#else
            GC.SuppressFinalize(this);

            await base.DisposeAsync().ConfigureAwait(false);
            await _stream.DisposeAsync().ConfigureAwait(false);

            _readCts?.Dispose();
            _writeCts?.Dispose();
#endif
        }
    }
}
