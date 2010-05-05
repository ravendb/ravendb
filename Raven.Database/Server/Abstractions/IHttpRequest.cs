using System;
using System.Collections.Specialized;
using System.IO;

namespace Raven.Database.Server.Abstractions
{
	public interface IHttpRequest
	{
		NameValueCollection Headers { get;  }
		Stream InputStream { get; }
		NameValueCollection QueryString { get; }
		string HttpMethod { get; }
		Uri Url { get; }
		string RawUrl { get; }
	}
}