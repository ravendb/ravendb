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
        private const int BufferSize = 4096;

        public AttachmentStreamContent(Stream stream, CancellationToken cancellationToken)
        {
            _stream = stream ?? throw new ArgumentNullException(nameof(stream));
            _cancellationToken = cancellationToken;
        }

        protected override Task SerializeToStreamAsync(Stream stream, TransportContext context)
        {
            Debug.Assert(stream != null);
            return _stream.CopyToAsync(stream, BufferSize, _cancellationToken);
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
