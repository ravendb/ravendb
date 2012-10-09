using System;
using System.Net;
using System.Text;

namespace Raven.Client.Silverlight.Connection
{
	public class HttpJsonRequestHelper
	{
		public static void CopyHeaders(HttpWebRequest src, HttpWebRequest dest)
		{
			foreach (string header in src.Headers)
			{
				var value = src.Headers[header];
				if (value == null)
					continue;
				switch (header)
				{
					case "Accept":
						dest.Accept = src.Accept;
						break;
					case "Connection":
						// explicitly ignoring this
						break;
					case "Content-Length":
						dest.ContentLength = src.ContentLength;
						break;
					case "Content-Type":
						dest.ContentType = src.ContentType;
						break;
					case "Date":
						break;
					case "Expect":
						// explicitly ignoring this
						break;
					case "Range":
						throw new NotSupportedException(
							"Range copying isn't supported at this stage, we don't support range queries anyway, so it shouldn't matter");
					case "User-Agent":
						dest.UserAgent = src.UserAgent;
						break;
					default:
						dest.Headers[header] = value;
						break;
				}
			}
		}

	}
}