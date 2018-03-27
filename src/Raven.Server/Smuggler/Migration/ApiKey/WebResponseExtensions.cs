using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;

namespace Raven.Server.Smuggler.Migration.ApiKey
{
    public static class WebResponseExtensions
    {
        /// <summary>
        /// Gets the response stream with HTTP decompression.
        /// </summary>
        /// <param name="response">The response.</param>
        /// <returns></returns>
        public static async Task<Stream> GetResponseStreamWithHttpDecompression(this HttpResponseMessage response)
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
