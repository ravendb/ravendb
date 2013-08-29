//-----------------------------------------------------------------------
// <copyright file="WebResponseExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

#if SILVERLIGHT
using Ionic.Zlib;
#else
using System.IO.Compression;
#endif

namespace Raven.Abstractions.Connection
{
	/// <summary>
	/// Extensions for web requests
	/// </summary>
	public static class WebResponseExtensions
	{
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
				stream = new GZipStream(stream, CompressionMode.Decompress);
			else if (encoding != null && encoding.Contains("deflate"))
				stream = new DeflateStream(stream, CompressionMode.Decompress);
			return stream;
		}

		/// <summary>
		/// Gets the response stream with HTTP decompression.
		/// </summary>
		/// <param name="response">The response.</param>
		/// <returns></returns>
		public static async Task<Stream> GetResponseStreamWithHttpDecompression(this HttpResponseMessage response)
		{
			var stream = await response.Content.ReadAsStreamAsync();
			var encoding = response.Content.Headers.ContentEncoding.FirstOrDefault();
			if (encoding != null && encoding.Contains("gzip"))
				stream = new GZipStream(stream, CompressionMode.Decompress);
			else if (encoding != null && encoding.Contains("deflate"))
				stream = new DeflateStream(stream, CompressionMode.Decompress);
			return stream;
		}
	}
}