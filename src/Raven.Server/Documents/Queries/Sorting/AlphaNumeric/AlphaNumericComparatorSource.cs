using System;
using System.Buffers;
using System.Collections.Generic;
using Lucene.Net.Search;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Queries.Sorting.AlphaNumeric
{
    public class AlphaNumericComparatorSource : FieldComparatorSource
    {
        private readonly DocumentsOperationContext _context;
        private readonly Func<int, string[]> _valuesArrayFactory;

        public AlphaNumericComparatorSource(DocumentsOperationContext context)
        {
            _context = context;
        }

        public override FieldComparator NewComparator(string fieldname, int numHits, int sortPos, bool reversed)
        {
            return new AlphaNumericFieldComparator(fieldname, _context.AllocatePooledArray<string>(numHits));
        }
    }
}
