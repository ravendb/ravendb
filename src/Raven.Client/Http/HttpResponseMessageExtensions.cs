using System.IO;
using System.IO.Compression;
using System.Net.Http;
using System.Threading.Tasks;
using System.Linq;

namespace Raven.Client.Http
{
    public static class HttpResponseMessageExtensions
    {
        public static string ReadToEnd(this Stream stream)
        {
            stream.Position = 0;
            using (var streamReader = new StreamReader(stream))
            {
                return streamReader.ReadToEnd();
            }
        }

        public static async Task<Stream> ReadAsStreamUncompressedAsync(this HttpResponseMessage response)
        {
            var stream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);

            var encoding = response.Content.Headers.ContentEncoding.FirstOrDefault();
            if (encoding != null && encoding.Contains("gzip"))
                stream = new GZipStream(stream, CompressionMode.Decompress);
            else if (encoding != null && encoding.Contains("deflate"))
                stream = new DeflateStream(stream, CompressionMode.Decompress);
            return stream;
        }
    }
}