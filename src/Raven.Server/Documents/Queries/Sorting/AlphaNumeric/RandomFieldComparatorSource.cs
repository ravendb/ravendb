using Lucene.Net.Search;

namespace Raven.Server.Documents.Queries.Sorting.AlphaNumeric
{
    public sealed class RandomFieldComparatorSource : FieldComparatorSource 
    {
        public static readonly RandomFieldComparatorSource Instance = new RandomFieldComparatorSource();

        private RandomFieldComparatorSource()
        {
        }

        public override FieldComparator NewComparator(string fieldname, int numHits, int sortPos, bool reversed)
        {
            return new RandomFieldComparator(fieldname, numHits);
        }
    }
}
