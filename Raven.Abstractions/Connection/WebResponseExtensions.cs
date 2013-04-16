//-----------------------------------------------------------------------
// <copyright file="WebResponseExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
#if NETFX_CORE
using System.Net.Http;
#endif

namespace Raven.Abstractions.Connection
{
	/// <summary>
	/// Extensions for web requests
	/// </summary>
	public static class WebResponseExtensions
	{
#if SILVERLIGHT
		/// <summary>
		/// Gets the response stream with HTTP decompression.
		/// </summary>
		/// <param name="response">The response.</param>
		/// <returns></returns>
		public static Stream GetResponseStreamWithHttpDecompression(this WebResponse response)
		{
			var stream = response.GetResponseStream();
			var encoding = response.Headers["Content-Encoding"];
			if (encoding != null && encoding.Contains("gzip"))
				stream = new Ionic.Zlib.GZipStream(stream, Ionic.Zlib.CompressionMode.Decompress);
			else if (encoding != null && encoding.Contains("deflate"))
				stream = new Ionic.Zlib.DeflateStream(stream, Ionic.Zlib.CompressionMode.Decompress);
			return stream;
		}
#else
		/// <summary>
		/// Gets the response stream with HTTP decompression.
		/// </summary>
		/// <param name="response">The response.</param>
		/// <returns></returns>
		public static Stream GetResponseStreamWithHttpDecompression(this WebResponse response)
		{
			var stream = response.GetResponseStream();
			Debug.Assert(stream != null, "stream != null");
			var encoding = response.Headers["Content-Encoding"];
			if (encoding != null && encoding.Contains("gzip"))
				stream = new System.IO.Compression.GZipStream(stream, System.IO.Compression.CompressionMode.Decompress);
			else if (encoding != null && encoding.Contains("deflate"))
				stream = new System.IO.Compression.DeflateStream(stream, System.IO.Compression.CompressionMode.Decompress);
			return stream;
		}
#endif

#if NETFX_CORE
		/// <summary>
		/// Gets the response stream with HTTP decompression.
		/// </summary>
		/// <param name="response">The response.</param>
		/// <returns></returns>
		public static Stream GetResponseStreamWithHttpDecompression(this HttpResponseMessage response)
		{
			var stream = response.Content.ReadAsStreamAsync().Result;
			var encoding = response.Headers.GetValues("Content-Encoding");
			if (encoding != null && encoding.Contains("gzip"))
				stream = new System.IO.Compression.GZipStream(stream, System.IO.Compression.CompressionMode.Decompress);
			else if (encoding != null && encoding.Contains("deflate"))
				stream = new System.IO.Compression.DeflateStream(stream, System.IO.Compression.CompressionMode.Decompress);
			return stream;
		}
#endif

	}
}
