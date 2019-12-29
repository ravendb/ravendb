using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.Util
{
    internal class StreamWithTimeout : Stream, IDisposable
    {
        private static readonly TimeSpan DefaultWriteTimeout = TimeSpan.FromSeconds(120);
        private static readonly TimeSpan DefaultReadTimeout = TimeSpan.FromSeconds(120);
        
        private readonly Stream _stream;
        private int _writeTimeout;
        private int _readTimeout;
        private bool _canBaseStreamTimeoutOnWrite;
        private bool _canBaseStreamTimeoutOnRead;
        private CancellationTokenSource _cts;
        public StreamWithTimeout(Stream stream, TimeSpan? writeTimeout = null, TimeSpan? readTimeout = null)
        {
            _stream = stream;
            SetWriteTimeoutIfNeeded(writeTimeout);
            SetReadTimeoutIfNeeded(readTimeout);
        }

        private void SetReadTimeoutIfNeeded(TimeSpan? readTimeout)
        {
            _canBaseStreamTimeoutOnRead = _stream.CanTimeout && _stream.ReadTimeout < int.MaxValue;

            if (_canBaseStreamTimeoutOnRead)
                _readTimeout = (int?)readTimeout?.TotalMilliseconds ?? _stream.ReadTimeout;
            else
                _readTimeout = (int)(readTimeout ?? DefaultReadTimeout).TotalMilliseconds;
        }

        private void SetWriteTimeoutIfNeeded(TimeSpan? writeTimeout)
        {
            _canBaseStreamTimeoutOnWrite = _stream.CanTimeout && _stream.WriteTimeout < int.MaxValue;

            if (_canBaseStreamTimeoutOnWrite)
                _writeTimeout = (int?)writeTimeout?.TotalMilliseconds ?? _stream.WriteTimeout;
            else
                _writeTimeout = (int)(writeTimeout ?? DefaultWriteTimeout).TotalMilliseconds;
        }

        public override int ReadTimeout => _readTimeout;

        public override int WriteTimeout => _writeTimeout;

        public override bool CanTimeout => true;

        public override void Flush()
        {
            _stream.Flush();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (_canBaseStreamTimeoutOnRead)
                return _stream.Read(buffer, offset, count);

            return AsyncHelpers.RunSync(() => ReadAsyncWithTimeout(buffer, offset, count, CancellationToken.None));
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            if (_canBaseStreamTimeoutOnRead)
                return _stream.ReadAsync(buffer, offset, count, cancellationToken);

            return ReadAsyncWithTimeout(buffer, offset, count, cancellationToken);
        }

        private Task<int> ReadAsyncWithTimeout(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            _cts?.Dispose();
            _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _cts.CancelAfter(_readTimeout);

            return _stream.ReadAsync(buffer, offset, count, _cts.Token);
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
            if (_canBaseStreamTimeoutOnWrite)
            {
                _stream.Write(buffer, offset, count);
                return;
            }

            AsyncHelpers.RunSync(() => WriteAsyncWithTimeout(buffer, offset, count, CancellationToken.None));
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            if (_canBaseStreamTimeoutOnWrite)
            {
                return _stream.WriteAsync(buffer, offset, count, cancellationToken);
            }

            return WriteAsyncWithTimeout(buffer, offset, count, cancellationToken);
        }

        private Task WriteAsyncWithTimeout(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            _cts?.Dispose();
            _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _cts.CancelAfter(_writeTimeout);

            return _stream.WriteAsync(buffer, offset, count, _cts.Token);
        }

        public override bool CanRead => _stream.CanRead;
        public override bool CanSeek => _stream.CanSeek;
        public override bool CanWrite => _stream.CanWrite;
        public override long Length => _stream.Length;
        public override long Position
        {
            get => _stream.Position;
            set => _stream.Position = value;
        }

        public new void Dispose()
        {
            base.Dispose(true);
            _stream.Dispose();
            _cts?.Dispose();
        }
    }
}
