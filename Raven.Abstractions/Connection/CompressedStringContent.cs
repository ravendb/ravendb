using System.IO;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.IO.Compression;
using System.Net.Http.Headers;

namespace Raven.Abstractions.Connection
{
    public class CompressedStringContent : HttpContent
    {
        private readonly string data;
        private readonly bool disableRequestCompression;

        public CompressedStringContent(string data, bool disableRequestCompression, string contentType = null)
        {
            this.data = data;
            this.disableRequestCompression = disableRequestCompression;

            if (disableRequestCompression == false)
            {
                Headers.ContentEncoding.Add("gzip");
            }

            if(string.IsNullOrWhiteSpace(contentType) == false)
                Headers.ContentType = new MediaTypeHeaderValue(contentType);
        }

        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext context)
        {
            if (disableRequestCompression == false)
                stream = new GZipStream(stream, CompressionMode.Compress, true);

            using (var streamWriter = new StreamWriter(stream, Encoding.UTF8, 4096, true))
            {
                await streamWriter.WriteAsync(data).ConfigureAwait(false);
                await streamWriter.FlushAsync().ConfigureAwait(false);
            }

            if (disableRequestCompression == false)
                stream.Dispose();
        }

        protected override bool TryComputeLength(out long length)
        {
            length = -1;
            return false;
        }
    }
}
