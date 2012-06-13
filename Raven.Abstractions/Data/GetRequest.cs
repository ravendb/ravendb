using System;
using System.Collections.Generic;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions.Data
{
	public class GetRequest
	{
		public string Url { get; set; }
		public IDictionary<string,string> Headers { get; set; }
		public string Query { get; set; }

		[JsonIgnore]
		public string UrlAndQuery
		{
			get
			{
				if (Query == null)
					return Url;
				if (Query.StartsWith("?"))
					return Url + Query;
				return Url + "?" + Query;
			}
		}

		public GetRequest()
		{
			Headers = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase);
		}
	}
}