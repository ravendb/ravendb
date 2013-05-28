using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.WinRT.Connection
{
	public class CompressedStringContent : HttpContent
	{
		private readonly string data;
		private readonly bool disableRequestCompression;

		public CompressedStringContent(string data, bool disableRequestCompression)
		{
			this.data = data;
			this.disableRequestCompression = disableRequestCompression;
		}

		protected override async Task SerializeToStreamAsync(Stream stream, TransportContext context)
		{
			var streamWriter = new StreamWriter(stream, Encoding.UTF8);
			await streamWriter.WriteAsync(data);
		}

		protected override async Task<Stream> CreateContentReadStreamAsync()
		{
			var stream = await base.CreateContentReadStreamAsync();

			if (disableRequestCompression)
				return stream;

			var gzipStream = new GZipStream(stream, CompressionMode.Compress);
			return gzipStream;
		}

		protected override bool TryComputeLength(out long length)
		{
			if (disableRequestCompression)
			{
				length = data.Length;
				return true;
			}

			length = -1;
			return false;
		}
	}
}