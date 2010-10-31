using System;
using System.Collections.Specialized;
using System.IO;

namespace Raven.Http.Abstractions
{
	public interface IHttpRequest
	{
        NameValueCollection Headers { get;  }
		Stream InputStream { get; }
		NameValueCollection QueryString { get; }
		string HttpMethod { get; }
        Uri Url { get; set; }
        string RawUrl { get; set; }
	}
}
