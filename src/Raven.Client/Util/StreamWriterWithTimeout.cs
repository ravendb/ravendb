using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Utils;

namespace Raven.Client.Util
{
    internal class StreamWriterWithTimeout : Stream, IDisposable
    {
        private static readonly TimeSpan DefaultWriteTimeout = TimeSpan.FromSeconds(120);
        
        private readonly Stream _stream;
        private readonly int _writeTimeout;
        private readonly bool _canBaseStreamTimeoutOnWrite;
        private CancellationTokenSource _cts;
        public StreamWriterWithTimeout(Stream stream, TimeSpan? writeTimeout = null)
        {
            _stream = stream;
            _canBaseStreamTimeoutOnWrite = _stream.CanTimeout && _stream.WriteTimeout < int.MaxValue;

            if (_canBaseStreamTimeoutOnWrite)
                _writeTimeout = (int?)writeTimeout?.TotalMilliseconds ?? _stream.WriteTimeout;
            else
                _writeTimeout = (int)(writeTimeout ?? DefaultWriteTimeout).TotalMilliseconds;
        }

        public override int WriteTimeout => _writeTimeout;

        public override bool CanTimeout => true;

        public override void Flush()
        {
            _stream.Flush();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        private Task WriteAsyncWithTimeout(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            _cts?.Dispose();
            _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _cts.CancelAfter(_writeTimeout);

            return _stream.WriteAsync(buffer, offset, count, _cts.Token);
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

        public override bool CanRead => false;
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
