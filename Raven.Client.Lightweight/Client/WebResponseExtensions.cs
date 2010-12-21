//-----------------------------------------------------------------------
// <copyright file="WebResponseExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.IO;
using System.IO.Compression;
using System.Net;

namespace Raven.Client.Client
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
			var encoding = response.Headers["Content-Encoding"];
			if (encoding != null && encoding.Contains("gzip"))
				stream = new GZipStream(stream, CompressionMode.Decompress);
			else if (encoding != null && encoding.Contains("deflate"))
				stream = new DeflateStream(stream, CompressionMode.Decompress);
			return stream;
		}
	}
}
