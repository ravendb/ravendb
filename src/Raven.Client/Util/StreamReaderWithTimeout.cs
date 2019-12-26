using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Utils;

namespace Raven.Client.Util
{
    internal class StreamReaderWithTimeout : Stream, IDisposable
    {
        private static readonly TimeSpan DefaultReadTimeout = TimeSpan.FromSeconds(60);

        private readonly Stream _stream;
        private readonly int _readTimeout;
        private readonly bool _canBaseStreamTimeoutOnRead;
        private CancellationTokenSource _cts;
        public StreamReaderWithTimeout(Stream stream, TimeSpan? readTimeout = null)
        {
            _stream = stream;
            _canBaseStreamTimeoutOnRead = _stream.CanTimeout && _stream.ReadTimeout < int.MaxValue;

            if (_canBaseStreamTimeoutOnRead)
                _readTimeout = _stream.ReadTimeout;
            else
                _readTimeout = (int)(readTimeout ?? DefaultReadTimeout).TotalMilliseconds;
        }

        public override int ReadTimeout => _readTimeout;

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
            if (_cts == null)
                _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

            _cts.Token.ThrowIfCancellationRequested();

            return _stream.ReadAsync(buffer, offset, count, _cts.Token).WaitForTaskCompletion(TimeSpan.FromMilliseconds(_readTimeout), _cts);
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
            throw new NotSupportedException();
        }

        public override bool CanRead => _stream.CanRead;
        public override bool CanSeek => _stream.CanSeek;
        public override bool CanWrite => false;
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
