using System.Collections.Generic;

namespace Raven.Abstractions.Data
{
	public class FacetResult
	{
		/// <summary>
		/// The facet terms and hits up to a limit of MaxResults items (as specified in the facet setup document), sorted
		/// in TermSortMode order (as indiciated in the facet setup document).
		/// </summary>
		public List<FacetValue> Values { get; set; }
		/// <summary>
		/// A list of remaining terms in term sort order for terms that are outside of the MaxResults count.
		/// </summary>
		public List<string> RemainingTerms { get; set; }
		/// <summary>
		/// The number of remaining terms outside of those covered by the Values terms.
		/// </summary>
		public int RemainingTermsCount { get; set; }
		/// <summary>
		/// The number of remaining hits outside of those covered by the Values terms.
		/// </summary>
		public int RemainingHits { get; set; }

		public FacetResult()
		{
			Values = new List<FacetValue>();
		}
	}
}
