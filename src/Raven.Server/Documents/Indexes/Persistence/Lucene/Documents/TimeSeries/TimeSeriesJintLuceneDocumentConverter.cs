using Raven.Client;
using System.Collections.Generic;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.TimeSeries
{
    public class TimeSeriesJintLuceneDocumentConverter : JintLuceneDocumentConverter
    {
        public TimeSeriesJintLuceneDocumentConverter(ICollection<IndexField> fields, bool indexImplicitNull = false, bool indexEmptyEntries = false)
            : base(fields, indexImplicitNull, indexEmptyEntries, storeValue: true, storeValueFieldName: Constants.Documents.Indexing.Fields.ValueFieldName)
        {
        }
    }
}
