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
    public sealed class AttachmentStreamContent : HttpContent
    {
        private readonly Stream _stream;
        private readonly CancellationToken _cancellationToken;
        private const int BufferSize = 4096;

        public AttachmentStreamContent(Stream stream, CancellationToken cancellationToken)
        {
            _stream = stream ?? throw new ArgumentNullException(nameof(stream));
            _cancellationToken = cancellationToken;
        }

        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext context)
        {
            Debug.Assert(stream != null);

            // Immediately flush request stream to send headers
            // https://github.com/dotnet/corefx/issues/39586#issuecomment-516210081
            // https://github.com/dotnet/runtime/issues/96223#issuecomment-1865009861
            await stream.FlushAsync(_cancellationToken).ConfigureAwait(false);

            await _stream.CopyToAsync(stream, BufferSize, _cancellationToken).ConfigureAwait(false);
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
