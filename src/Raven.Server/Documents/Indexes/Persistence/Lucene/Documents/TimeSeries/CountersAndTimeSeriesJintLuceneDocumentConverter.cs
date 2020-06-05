using Raven.Client;
using Raven.Server.Documents.Indexes.Static.Counters;
using Raven.Server.Documents.Indexes.Static.TimeSeries;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.TimeSeries
{
    public sealed class CountersAndTimeSeriesJintLuceneDocumentConverter : JintLuceneDocumentConverterBase
    {
        public CountersAndTimeSeriesJintLuceneDocumentConverter(MapTimeSeriesIndex index)
            : base(index, index.Definition.IndexDefinition, numberOfBaseFields: 3, keyFieldName: Constants.Documents.Indexing.Fields.DocumentIdFieldName, storeValue: true, storeValueFieldName: Constants.Documents.Indexing.Fields.ValueFieldName)
        {
        }

        public CountersAndTimeSeriesJintLuceneDocumentConverter(MapCountersIndex index)
            : base(index, index.Definition.IndexDefinition, numberOfBaseFields: 3, keyFieldName: Constants.Documents.Indexing.Fields.DocumentIdFieldName, storeValue: true, storeValueFieldName: Constants.Documents.Indexing.Fields.ValueFieldName)
        {
        }

        public CountersAndTimeSeriesJintLuceneDocumentConverter(MapReduceTimeSeriesIndex index)
            : base(index, index.Definition.IndexDefinition, numberOfBaseFields: 3, keyFieldName: Constants.Documents.Indexing.Fields.DocumentIdFieldName, storeValue: true, storeValueFieldName: Constants.Documents.Indexing.Fields.ValueFieldName)
        {
        }

        public CountersAndTimeSeriesJintLuceneDocumentConverter(MapReduceCountersIndex index)
            : base(index, index.Definition.IndexDefinition, numberOfBaseFields: 3, keyFieldName: Constants.Documents.Indexing.Fields.DocumentIdFieldName, storeValue: true, storeValueFieldName: Constants.Documents.Indexing.Fields.ValueFieldName)
        {
        }
    }
}
