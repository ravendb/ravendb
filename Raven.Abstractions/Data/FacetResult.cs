using System.Collections.Generic;

namespace Raven.Abstractions.Data
{
	public class FacetResult
	{
		public List<FacetValue> Values { get; set; }
		public List<string> Terms { get; set; }

		public FacetResult()
		{
			Values = new List<FacetValue>();
			Terms = new List<string>();
		}
	}
}
