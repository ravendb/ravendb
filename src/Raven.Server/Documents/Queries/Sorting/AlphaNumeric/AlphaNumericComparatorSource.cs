using Lucene.Net.Search;

namespace Raven.Server.Documents.Queries.Sorting.AlphaNumeric
{
    public class AlphaNumericComparatorSource : FieldComparatorSource
    {
        public override FieldComparator NewComparator(string fieldname, int numHits, int sortPos, bool reversed)
        {
            return new AlphaNumericFieldComparator(numHits, fieldname);
        }
    }
}
