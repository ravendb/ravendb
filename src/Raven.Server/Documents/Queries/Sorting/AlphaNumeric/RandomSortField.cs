using Lucene.Net.Search;

namespace Raven.Server.Documents.Queries.Sorting.AlphaNumeric
{
    public class RandomSortField : SortField
    {
        public RandomSortField(string field) : base(field, INT)
        {
        }

        public override FieldComparator GetComparator(int numHits, int sortPos)
        {
            // sortPost and reversed are ignored by the RandomFieldComparator
            return ComparatorSource.NewComparator(Field, numHits, sortPos, reversed: false);
        }

        public override FieldComparatorSource ComparatorSource => RandomFieldComparatorSource.Instance;
    }
}
