using Lucene.Net.Search;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Queries.Sorting.AlphaNumeric
{
    public class AlphaNumericComparatorSource : FieldComparatorSource
    {
        private readonly DocumentsOperationContext _context;

        public AlphaNumericComparatorSource(DocumentsOperationContext context)
        {
            _context = context;
        }

        public override FieldComparator NewComparator(string fieldname, int numHits, int sortPos, bool reversed)
        {
            return new AlphaNumericFieldComparator(fieldname, numHits);
        }
    }
}
