using System;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;

namespace Raven.Client.Json
{
    internal sealed class BlittableJsonContent : HttpContent
    {
        private readonly Func<Stream, Task> _asyncTaskWriter;
        private readonly DocumentConventions _conventions;

        public BlittableJsonContent(Func<Stream, Task> writer, DocumentConventions conventions)
        {
            _asyncTaskWriter = writer ?? throw new ArgumentNullException(nameof(writer));
            _conventions = conventions;

            if (_conventions.UseHttpCompression)
                Headers.ContentEncoding.Add("gzip");
        }

        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext context)
        {
            if (_conventions.UseHttpCompression == false)
            {
                await _asyncTaskWriter(stream).ConfigureAwait(false);
                return;
            }

#if NETSTANDARD2_0 || NETCOREAPP2_1
            using (var gzipStream = new GZipStream(stream, CompressionMode.Compress, leaveOpen: true))
#else
            await using (var gzipStream = new GZipStream(stream, CompressionMode.Compress, leaveOpen: true))
#endif
            {
                await _asyncTaskWriter(gzipStream).ConfigureAwait(false);
            }
        }

        protected override bool TryComputeLength(out long length)
        {
            length = -1;
            return false;
        }
    }
}
