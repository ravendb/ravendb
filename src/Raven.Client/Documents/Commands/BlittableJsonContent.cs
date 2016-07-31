using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class BlittableJsonContent : HttpContent
    {
        private readonly BlittableJsonReaderObject _document;
        private readonly JsonOperationContext _context;

        public BlittableJsonContent(BlittableJsonReaderObject document, JsonOperationContext context)
        {
            _document = document;
            _context = context;
            Headers.ContentEncoding.Add("gzip");
        }

        protected override Task SerializeToStreamAsync(Stream stream, TransportContext context)
        {
            using (var gzipStream = new GZipStream(stream, CompressionMode.Compress, leaveOpen: true))
            {
                _context.Write(gzipStream, _document);
            }
            return Task.CompletedTask;
        }

        protected override bool TryComputeLength(out long length)
        {
            if (_document == null)
            {
                length = 0;
                return true;
            }

            length = -1;
            return false;
        }
    }
}