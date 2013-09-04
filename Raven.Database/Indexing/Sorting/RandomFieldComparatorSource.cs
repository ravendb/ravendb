using Lucene.Net.Search;

namespace Raven.Database.Indexing.Sorting
{
	public class RandomFieldComparatorSource : FieldComparatorSource 
	{
		public override FieldComparator NewComparator(string fieldname, int numHits, int sortPos, bool reversed)
		{
			return new RandomFieldComparator(numHits, fieldname);
		}
	}
}