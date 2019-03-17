using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.Http
{
    internal class IdleTimeoutHttpContent : HttpContent
    {
        private static readonly TimeSpan TimeoutThreshold = TimeSpan.FromMilliseconds(RequestExecutor.DefaultHttpClientTimeout.TotalMilliseconds / 4);

        private readonly HttpContent _content;
        private readonly CancellationTokenSource _cts;

        public IdleTimeoutHttpContent(HttpContent content, CancellationTokenSource cts)
        {
            _content = content;
            _cts = cts;

            if (_content?.Headers == null)
                return;

            foreach (var header in _content.Headers)
                Headers.TryAddWithoutValidation(header.Key, header.Value);
        }

        protected override Task SerializeToStreamAsync(Stream stream, TransportContext context)
        {
            if (_content == null)
                return Task.CompletedTask;

            return _content.CopyToAsync(new IdleTimeoutStream(stream, _cts), context);
        }

        protected override bool TryComputeLength(out long length)
        {
            length = -1;
            return false;
        }

        protected override void Dispose(bool disposing)
        {
            _content.Dispose();

            base.Dispose(disposing);
        }

        internal class IdleTimeoutStream : Stream
        {
            private readonly Stream _stream;
            private readonly CancellationTokenSource _cts;
            private Stopwatch _sw;

            public IdleTimeoutStream(Stream stream, CancellationTokenSource cts)
            {
                _stream = stream ?? throw new ArgumentNullException(nameof(stream));
                _cts = cts ?? throw new ArgumentNullException(nameof(cts));
            }

            public override void Flush()
            {
                HandleTimeout();

                _stream.Flush();
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                HandleTimeout();

                return _stream.Read(buffer, offset, count);
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
                HandleTimeout();

                _stream.Write(buffer, offset, count);
            }

            protected override void Dispose(bool disposing)
            {
                _stream.Dispose();

                base.Dispose(disposing);
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

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void HandleTimeout()
            {
                if (_sw == null)
                {
                    _sw = Stopwatch.StartNew();
                    return;
                }

                if (_sw.Elapsed <= TimeoutThreshold) 
                    return;

                _cts.CancelAfter(RequestExecutor.DefaultHttpClientTimeout);
                _sw.Restart();
            }
        }
    }
}
