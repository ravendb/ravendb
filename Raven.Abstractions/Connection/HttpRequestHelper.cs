using System;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Text;

namespace Raven.Abstractions.Connection
{
	public class HttpRequestHelper
	{
		public static void WriteDataToRequest(HttpWebRequest req, string data, bool disableCompression)
		{
			req.SendChunked = true;
			using (var requestStream = req.GetRequestStream())
			using (var dataStream = new GZipStream(requestStream, CompressionMode.Compress))
			using (var writer = disableCompression == false ?
					new StreamWriter(dataStream, Encoding.UTF8) :
					new StreamWriter(requestStream, Encoding.UTF8))
			{

				writer.Write(data);

				writer.Flush();

				if (disableCompression == false)
					dataStream.Flush();
				requestStream.Flush();
			}
		}

		public static void CopyHeaders(HttpWebRequest src, HttpWebRequest dest)
		{
			foreach (string header in src.Headers)
			{
				var values = src.Headers.GetValues(header);
				if (values == null)
					continue;
				if (WebHeaderCollection.IsRestricted(header))
				{
					switch (header)
					{
						case "Accept":
							dest.Accept = src.Accept;
							break;
						case "Connection":
							// explicitly ignoring this
							break;
						case "Content-Length":
							break;
						case "Content-Type":
							dest.ContentType = src.ContentType;
							break;
						case "Date":
							break;
						case "Expect":
							// explicitly ignoring this
							break;
#if !NET35
						case "Host":
							dest.Host = src.Host;
							break;
#endif
						case "If-Modified-Since":
							dest.IfModifiedSince = src.IfModifiedSince;
							break;
						case "Range":
							throw new NotSupportedException("Range copying isn't supported at this stage, we don't support range queries anyway, so it shouldn't matter");
						case "Referer":
							dest.Referer = src.Referer;
							break;
						case "Transfer-Encoding":
							dest.SendChunked = src.SendChunked;

							break;
						case "User-Agent":
							dest.UserAgent = src.UserAgent;
							break;
						case "Proxy-Connection":
							dest.Proxy = src.Proxy;
							break;
						default:
							throw new ArgumentException(string.Format("No idea how to handle restricted header: '{0}'", header));
					}
				}
				else
				{
					foreach (var value in values)
					{
						dest.Headers.Add(header, value);
					}
				}
			}
		}
	}
}