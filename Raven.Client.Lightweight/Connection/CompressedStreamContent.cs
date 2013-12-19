using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

#if SILVERLIGHT
using Ionic.Zlib;
#else
using System.IO.Compression;
#endif

namespace Raven.Client.Connection
{
	public class CompressedStreamContent : HttpContent
	{
        private readonly Stream data;
		private readonly bool disableRequestCompression;

		public CompressedStreamContent(Stream data, bool disableRequestCompression)
		{
		    if (data == null) throw new ArgumentNullException("data");
		    this.data = data;
			this.disableRequestCompression = disableRequestCompression;

			if (disableRequestCompression == false)
			{
				Headers.ContentEncoding.Add("gzip");
			}
		}

		protected override async Task SerializeToStreamAsync(Stream stream, TransportContext context)
		{
			try
			{
				if (disableRequestCompression == false)
					stream = new GZipStream(stream, CompressionMode.Compress, leaveOpen: true);

				await data.CopyToAsync(stream);
				await stream.FlushAsync();
			}
			finally
			{
				if (disableRequestCompression == false)
					stream.Dispose();
			}
		}

		protected override bool TryComputeLength(out long length)
		{
		    length = -1;
		    return false;
		}
	}
}