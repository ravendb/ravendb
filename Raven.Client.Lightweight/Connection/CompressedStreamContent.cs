using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

using System.IO.Compression;

namespace Raven.Client.Connection
{
	public class CompressedStreamContent : HttpContent
	{
	    private readonly bool disposeStream;
        private readonly Stream data;
		private readonly bool disableRequestCompression;

		public CompressedStreamContent(Stream data, bool disableRequestCompression, bool disposeStream = true)
		{
		    if (data == null) throw new ArgumentNullException("data");
		    this.data = data;
			this.disableRequestCompression = disableRequestCompression;
		    this.disposeStream = disposeStream;

			if (disableRequestCompression == false)
			{
				Headers.ContentEncoding.Add("gzip");
			}

			Disposables = new List<IDisposable>();
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

		protected override void Dispose(bool disposing)
		{
			if (disposeStream && data != null)
				data.Dispose();

			if (Disposables != null)
			foreach (var dispose in Disposables)
			{
				dispose.Dispose();
			}

			base.Dispose(disposing);
		}

		public List<IDisposable> Disposables { get; private set; }
	}
}