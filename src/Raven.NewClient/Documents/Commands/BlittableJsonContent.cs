using System;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Sparrow.Json;

namespace Raven.NewClient.Client.Documents.Commands
{
    public class BlittableJsonContent : HttpContent
    {
        private readonly Action<Stream> _writer;

        public BlittableJsonContent(Action<Stream> writer)
        {
            _writer = writer;
            Headers.ContentEncoding.Add("gzip");
        }

        protected override Task SerializeToStreamAsync(Stream stream, TransportContext context)
        {
            using (var gzipStream = new GZipStream(stream, CompressionMode.Compress, leaveOpen: true))
            {
                _writer(gzipStream);
            }
            return Task.CompletedTask;
        }

        protected override bool TryComputeLength(out long length)
        {
            length = -1;
            return false;
        }
    }
}