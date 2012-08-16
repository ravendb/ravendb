using System.Collections.Generic;

namespace Raven.Abstractions.Data
{
	public class FacetResult
	{
		public List<FacetValue> Values { get; set; }
		public List<string> RemainingTerms { get; set; }
		public int RemainingTermsCount { get; set; }
		public int RemainingHits { get; set; }

		public FacetResult()
		{
			Values = new List<FacetValue>();
		}
	}
}
