using System.Collections.Generic;

namespace Raven.Abstractions.Data
{
	public class Facet
	{
		public FacetMode Mode { get; set; }
		public string Name { get; set; }
		public List<string> Ranges { get; set; }
		public int? MaxResults { get; set; }
		public FacetTermSortMode TermSortMode { get; set; }
		public bool InclueRemainingTerms { get; set; }

		public Facet()
		{
			Ranges = new List<string>();
			TermSortMode = FacetTermSortMode.ValueAsc;
		}
	}
}