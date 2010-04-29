using System;
using System.Collections.Specialized;
using System.IO;

namespace Raven.Database.Abstractions
{
	public interface IHttpRequest
	{
		NameValueCollection Headers { get;  }
		Stream InputStream { get; }
		NameValueCollection QueryString { get; }
		Uri Url { get; }
		string HttpMethod { get; }
		string RawUrl { get; }
	}
}