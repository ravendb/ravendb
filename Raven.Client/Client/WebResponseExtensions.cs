using System.IO;
using System.IO.Compression;
using System.Net;

namespace Raven.Client.Client
{
	public static class WebResponseExtensions
	{
		public static Stream GetResponseStreamWithHttpDecompression(this WebResponse response)
		{
			var stream = response.GetResponseStream();
			var encoding = response.Headers["Content-Encoding"];
			if (encoding != null && encoding.Contains("gzip"))
				stream = new GZipStream(stream, CompressionMode.Decompress);
			else if (encoding != null && encoding.Contains("deflate"))
				stream = new DeflateStream(stream, CompressionMode.Decompress);
			return stream;
		}
	}
}