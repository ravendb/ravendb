using System;
using System.Collections.Generic;
using Raven.Json.Linq;

namespace Raven.Abstractions.Data
{
	public class GetResponse
	{
		public GetResponse()
		{
			Headers = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase);
		}

		public RavenJToken Result { get; set; }
		public IDictionary<string,string> Headers { get; set; }
		public int Status { get; set; }

		public bool RequestHasErrors()
		{
			switch (Status)
			{
				case 0:   // aggressively cached
				case 200: // known non error values
				case 201:
				case 203:
				case 204:
				case 304:
				case 404:
					return false;
				default:
					return true;
			}
		}
	}
}