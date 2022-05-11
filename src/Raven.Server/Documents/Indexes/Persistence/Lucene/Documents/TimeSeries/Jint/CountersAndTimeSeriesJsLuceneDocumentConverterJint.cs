using Raven.Client;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.Jint;
using Raven.Server.Documents.Indexes.Static.Counters;
using Raven.Server.Documents.Indexes.Static.TimeSeries;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.TimeSeries.Jint
{
    public sealed class CountersAndTimeSeriesJsLuceneDocumentConverterJint : JsLuceneDocumentConverterBaseJint
    {
        public CountersAndTimeSeriesJsLuceneDocumentConverterJint(MapTimeSeriesIndex index)
            : base(index, index.Definition.IndexDefinition, numberOfBaseFields: 3, keyFieldName: Constants.Documents.Indexing.Fields.DocumentIdFieldName, storeValue: true, storeValueFieldName: Constants.Documents.Indexing.Fields.ValueFieldName)
        {
        }

        public CountersAndTimeSeriesJsLuceneDocumentConverterJint(MapCountersIndex index)
            : base(index, index.Definition.IndexDefinition, numberOfBaseFields: 3, keyFieldName: Constants.Documents.Indexing.Fields.DocumentIdFieldName, storeValue: true, storeValueFieldName: Constants.Documents.Indexing.Fields.ValueFieldName)
        {
        }

        public CountersAndTimeSeriesJsLuceneDocumentConverterJint(MapReduceTimeSeriesIndex index)
            : base(index, index.Definition.IndexDefinition, numberOfBaseFields: 3, keyFieldName: Constants.Documents.Indexing.Fields.DocumentIdFieldName, storeValue: true, storeValueFieldName: Constants.Documents.Indexing.Fields.ValueFieldName)
        {
        }

        public CountersAndTimeSeriesJsLuceneDocumentConverterJint(MapReduceCountersIndex index)
            : base(index, index.Definition.IndexDefinition, numberOfBaseFields: 3, keyFieldName: Constants.Documents.Indexing.Fields.DocumentIdFieldName, storeValue: true, storeValueFieldName: Constants.Documents.Indexing.Fields.ValueFieldName)
        {
        }
    }
}
