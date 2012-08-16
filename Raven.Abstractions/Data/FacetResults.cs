using System.Collections.Generic;

namespace Raven.Abstractions.Data
{
	public class FacetResults
	{
		/// <summary>
		/// A list of results for the facet.  One entry for each term/range as specified in the facet setup document.
		/// </summary>
		public Dictionary<string, FacetResult> Results { get; set; }

		public FacetResults()
		{
			Results = new Dictionary<string, FacetResult>();
		}
	}
}
