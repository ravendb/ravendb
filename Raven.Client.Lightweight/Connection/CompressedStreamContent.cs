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

		public CompressedStreamContent(Stream data)
		{
		    if (data == null) throw new ArgumentNullException("data");
		    this.data = data;
			Headers.ContentEncoding.Add("gzip");
		}

		protected override async Task SerializeToStreamAsync(Stream stream, TransportContext context)
		{
            using (data)
			using (var dataStream = new GZipStream(stream, CompressionMode.Compress, true))
			{
			    await data.CopyToAsync(dataStream);
				await dataStream.FlushAsync();
			}
		}

		protected override bool TryComputeLength(out long length)
		{
		    length = -1;
		    return false;
		}
	}
}