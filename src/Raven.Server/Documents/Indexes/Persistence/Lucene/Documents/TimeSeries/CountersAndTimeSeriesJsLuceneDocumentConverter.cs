using Raven.Client;
using Raven.Server.Documents.Indexes.Static.Counters;
using Raven.Server.Documents.Indexes.Static.TimeSeries;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.TimeSeries
{
    public sealed class CountersAndTimeSeriesJsLuceneDocumentConverter : JsLuceneDocumentConverterBase
    {
        public CountersAndTimeSeriesJsLuceneDocumentConverter(MapTimeSeriesIndex index)
            : base(index, index.Definition.IndexDefinition, numberOfBaseFields: 3, keyFieldName: Constants.Documents.Indexing.Fields.DocumentIdFieldName, storeValue: true, storeValueFieldName: Constants.Documents.Indexing.Fields.ValueFieldName)
        {
        }

        public CountersAndTimeSeriesJsLuceneDocumentConverter(MapCountersIndex index)
            : base(index, index.Definition.IndexDefinition, numberOfBaseFields: 3, keyFieldName: Constants.Documents.Indexing.Fields.DocumentIdFieldName, storeValue: true, storeValueFieldName: Constants.Documents.Indexing.Fields.ValueFieldName)
        {
        }

        public CountersAndTimeSeriesJsLuceneDocumentConverter(MapReduceTimeSeriesIndex index)
            : base(index, index.Definition.IndexDefinition, numberOfBaseFields: 3, keyFieldName: Constants.Documents.Indexing.Fields.DocumentIdFieldName, storeValue: true, storeValueFieldName: Constants.Documents.Indexing.Fields.ValueFieldName)
        {
        }

        public CountersAndTimeSeriesJsLuceneDocumentConverter(MapReduceCountersIndex index)
            : base(index, index.Definition.IndexDefinition, numberOfBaseFields: 3, keyFieldName: Constants.Documents.Indexing.Fields.DocumentIdFieldName, storeValue: true, storeValueFieldName: Constants.Documents.Indexing.Fields.ValueFieldName)
        {
        }
    }
}
