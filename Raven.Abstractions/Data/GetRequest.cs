using System;
using System.Collections.Specialized;

namespace Raven.Abstractions.Data
{
	public class GetRequest
	{
		public string Url { get; set; }
		public NameValueCollection Headers { get; set; }
		public string Query { get; set; }

		public GetRequest()
		{
			Headers = new NameValueCollection();
		}
	}

	public class GetResponse
	{
		public GetResponse()
		{
			Headers = new NameValueCollection();
		}

		public string Result { get; set; }
		public NameValueCollection Headers { get; set; }
		public int Status { get; set; }
	}
}