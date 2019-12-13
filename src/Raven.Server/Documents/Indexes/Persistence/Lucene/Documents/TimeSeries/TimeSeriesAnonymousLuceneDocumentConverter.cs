using Raven.Client;
using System.Collections.Generic;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.TimeSeries
{
    public class TimeSeriesAnonymousLuceneDocumentConverter : AnonymousLuceneDocumentConverter
    {
        public TimeSeriesAnonymousLuceneDocumentConverter(ICollection<IndexField> fields, bool isMultiMap, bool indexImplicitNull = false, bool indexEmptyEntries = false)
            : base(fields, isMultiMap, indexImplicitNull, indexEmptyEntries, keyFieldName: Constants.Documents.Indexing.Fields.DocumentIdFieldName, storeValue: true, storeValueFieldName: Constants.Documents.Indexing.Fields.ValueFieldName)
        {
        }
    }
}
