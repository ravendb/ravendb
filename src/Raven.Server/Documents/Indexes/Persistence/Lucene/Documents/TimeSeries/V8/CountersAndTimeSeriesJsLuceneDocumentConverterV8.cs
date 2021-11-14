using Raven.Client;
using Raven.Server.Documents.Indexes.Static.Counters;
using Raven.Server.Documents.Indexes.Static.TimeSeries;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.V8;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.TimeSeries.V8
{
    public sealed class CountersAndTimeSeriesJsLuceneDocumentConverterV8 : JsLuceneDocumentConverterBaseV8
    {
        public CountersAndTimeSeriesJsLuceneDocumentConverterV8(MapTimeSeriesIndex index)
            : base(index, index.Definition.IndexDefinition, numberOfBaseFields: 3, keyFieldName: Constants.Documents.Indexing.Fields.DocumentIdFieldName, storeValue: true, storeValueFieldName: Constants.Documents.Indexing.Fields.ValueFieldName)
        {
        }

        public CountersAndTimeSeriesJsLuceneDocumentConverterV8(MapCountersIndex index)
            : base(index, index.Definition.IndexDefinition, numberOfBaseFields: 3, keyFieldName: Constants.Documents.Indexing.Fields.DocumentIdFieldName, storeValue: true, storeValueFieldName: Constants.Documents.Indexing.Fields.ValueFieldName)
        {
        }

        public CountersAndTimeSeriesJsLuceneDocumentConverterV8(MapReduceTimeSeriesIndex index)
            : base(index, index.Definition.IndexDefinition, numberOfBaseFields: 3, keyFieldName: Constants.Documents.Indexing.Fields.DocumentIdFieldName, storeValue: true, storeValueFieldName: Constants.Documents.Indexing.Fields.ValueFieldName)
        {
        }

        public CountersAndTimeSeriesJsLuceneDocumentConverterV8(MapReduceCountersIndex index)
            : base(index, index.Definition.IndexDefinition, numberOfBaseFields: 3, keyFieldName: Constants.Documents.Indexing.Fields.DocumentIdFieldName, storeValue: true, storeValueFieldName: Constants.Documents.Indexing.Fields.ValueFieldName)
        {
        }
    }
}
