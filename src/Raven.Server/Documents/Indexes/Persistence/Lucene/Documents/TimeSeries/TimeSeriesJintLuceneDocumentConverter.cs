using Raven.Client;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Static;
using System.Collections.Generic;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.TimeSeries
{
    public sealed class TimeSeriesJintLuceneDocumentConverter : JintLuceneDocumentConverterBase
    {
        public TimeSeriesJintLuceneDocumentConverter(ICollection<IndexField> fields, MapIndexDefinition definition, bool indexImplicitNull = false, bool indexEmptyEntries = false)
            : base(fields, definition.IndexDefinition, indexImplicitNull, indexEmptyEntries, keyFieldName: Constants.Documents.Indexing.Fields.DocumentIdFieldName, storeValue: true, storeValueFieldName: Constants.Documents.Indexing.Fields.ValueFieldName)
        {
        }

        public TimeSeriesJintLuceneDocumentConverter(ICollection<IndexField> fields, MapReduceIndexDefinition definition, bool indexImplicitNull = false, bool indexEmptyEntries = false)
            : base(fields, definition.IndexDefinition, indexImplicitNull, indexEmptyEntries, keyFieldName: Constants.Documents.Indexing.Fields.DocumentIdFieldName, storeValue: true, storeValueFieldName: Constants.Documents.Indexing.Fields.ValueFieldName)
        {
        }
    }
}
