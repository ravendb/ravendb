using Raven.Abstractions.Indexing;

namespace Raven.Studio.Features.Indexes
{
    public class FieldProperties
	{
		public string Name { get; set; }

		public FieldStorage Storage { get; set; }

		public FieldIndexing Indexing { get; set; }

		public SortOptions Sort { get; set; }

		public string Analyzer { get; set; }
	}
}