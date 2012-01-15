using Lucene.Net.Search;

namespace Raven.Database.Indexing.Sorting
{
	public class RandomSortField : SortField
	{
		public RandomSortField(string field) : base(field, INT)
		{
		}

		public override FieldComparator GetComparator(int numHits, int sortPos)
		{
			return new RandomFieldComparator(numHits, GetField());
		}

		public override FieldComparatorSource GetComparatorSource()
		{
			return new RandomFieldComparatorSource();
		}
	}
}