using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands.Batches;

namespace Raven.Client.Json
{
    public class GzipStreamContent : StreamContent
    {
        public GzipStreamContent(Stream content, string type)
            : base(content)
        {
            Headers.ContentEncoding.Add("gzip");
            Headers.TryAddWithoutValidation("Command-Type", type);
        }

        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext context)
        {
            using (var gzipStream = new GZipStream(stream, CompressionMode.Compress, leaveOpen: true))
            {
                await base.SerializeToStreamAsync(gzipStream, context);
            }
        }

        protected override bool TryComputeLength(out long length)
        {
            length = -1;
            return false;
        }
    }
}