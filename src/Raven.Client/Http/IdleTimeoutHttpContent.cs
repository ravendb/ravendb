using System;
using System.Buffers;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.Http
{
    internal class IdleTimeoutHttpContent : HttpContent
    {
        private static readonly TimeSpan TimeoutThreshold = TimeSpan.FromMilliseconds(RequestExecutor.DefaultHttpClientTimeout.TotalMilliseconds / 4);

        private readonly HttpContent _content;
        private readonly CancellationTokenSource _cts;
        private readonly byte[] _buffer;
        private const int BufferSize = 4096;

        public IdleTimeoutHttpContent(HttpContent content, CancellationTokenSource cts)
        {
            _content = content;
            _cts = cts;
            _buffer = ArrayPool<byte>.Shared.Rent(BufferSize);

            if (_content?.Headers == null)
                return;

            foreach (var header in _content.Headers)
                Headers.TryAddWithoutValidation(header.Key, header.Value);
        }

        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext context)
        {
            if (_content == null)
                return;

            var sw = Stopwatch.StartNew();

            var innerStream = await _content.ReadAsStreamAsync().ConfigureAwait(false);
            while (true)
            {
                var read = innerStream.Read(_buffer, 0, BufferSize);
                if (read == 0)
                    break;

                await stream.WriteAsync(_buffer, 0, read).ConfigureAwait(false);

                if (sw.Elapsed > TimeoutThreshold)
                {
                    _cts.CancelAfter(RequestExecutor.DefaultHttpClientTimeout);
                    sw.Restart();
                }
            }
        }

        protected override bool TryComputeLength(out long length)
        {
            length = -1;
            return false;
        }

        protected override void Dispose(bool disposing)
        {
            ArrayPool<byte>.Shared.Return(_buffer);

            _content.Dispose();

            base.Dispose(disposing);
        }
    }
}
