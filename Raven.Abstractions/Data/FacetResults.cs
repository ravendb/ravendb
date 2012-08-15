using System.Collections.Generic;

namespace Raven.Abstractions.Data
{
	public class FacetResults
	{
		public Dictionary<string, FacetResult> Results { get; set; }

		public FacetResults()
		{
			Results = new Dictionary<string, FacetResult>();
		}
	}
}
