using System;
using System.Collections.Generic;

namespace Raven.Abstractions.Data
{
	public class GetResponse
	{
		public GetResponse()
		{
			Headers = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase);
		}

		public string Result { get; set; }
		public IDictionary<string,string> Headers { get; set; }
		public int Status { get; set; }

		public bool RequestHasErrors()
		{
			switch (Status)
			{
				case 200: // known non error values
				case 203:
				case 304:
				case 404:
					return false;
				default:
					return true;
			}
		}
	}
}