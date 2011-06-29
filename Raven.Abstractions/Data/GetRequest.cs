using System;
using System.Collections.Generic;
using System.Collections.Specialized;

namespace Raven.Abstractions.Data
{
	public class GetRequest
	{
		public string Url { get; set; }
		public IDictionary<string,string> Headers { get; set; }
		public string Query { get; set; }

		public GetRequest()
		{
			Headers = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase);
		}
	}
}