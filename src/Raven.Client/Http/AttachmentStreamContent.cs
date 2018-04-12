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
    /// <summary>
    /// We use AttachmentStreamContent instead of StreamContent in order to not dispose the stream the user passes us
    /// so it will be usable again in failover scenarios.
    /// </summary>
    public class AttachmentStreamContent : HttpContent
    {
        private readonly Stream _stream;
        private readonly CancellationToken _cancellationToken;

        public AttachmentStreamContent(Stream stream, CancellationToken cancellationToken)
        {
            _stream = stream ?? throw new ArgumentNullException(nameof(stream));
            _cancellationToken = cancellationToken;
        }

        protected async Task CopyToStreamAsync(Stream stream)
        {
            var buffer = ArrayPool<byte>.Shared.Rent(4096);

            try
            {
                var count = await _stream.ReadAsync(buffer, 0, buffer.Length, _cancellationToken).ConfigureAwait(false);
                while (count > 0)
                {
                    await stream.WriteAsync(buffer, 0, count, _cancellationToken).ConfigureAwait(false);
                    count = await _stream.ReadAsync(buffer, 0, buffer.Length, _cancellationToken).ConfigureAwait(false);
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        protected override Task SerializeToStreamAsync(Stream stream, TransportContext context)
        {
            Debug.Assert(stream != null);
            // instead of : "_stream.CopyToAsync(stream, BufferSize, _cancellationToken)" - RavenDB-7291, we do:
            return CopyToStreamAsync(stream);
        }

        protected override bool TryComputeLength(out long length)
        {
            if (_stream.CanSeek)
            {
                length = _stream.Length;
                return true;
            }

            length = 0;
            return false;
        }

        protected override Task<Stream> CreateContentReadStreamAsync()
        {
            return Task.FromResult(_stream);
        }
    }
}
