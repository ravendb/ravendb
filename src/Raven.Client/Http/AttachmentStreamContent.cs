using System;
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
        private readonly byte[] _buffer = new byte[4096];

        private readonly Stream _stream;
        private readonly CancellationToken _cancellationToken;

        public AttachmentStreamContent(Stream stream, CancellationToken cancellationToken)
        {
            if (stream == null)
                throw new ArgumentNullException(nameof(stream));

            _stream = stream;
            _cancellationToken = cancellationToken;
        }

        protected async Task CopyToStreamAsync(Stream stream)
        {
            var count = await stream.ReadAsync(_buffer, 0, _buffer.Length, _cancellationToken).ConfigureAwait(false); 
            while (count > 0)
            {
                await _stream.WriteAsync(_buffer, 0, count, _cancellationToken);
                count = await stream.ReadAsync(_buffer, 0, _buffer.Length, _cancellationToken).ConfigureAwait(false);
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