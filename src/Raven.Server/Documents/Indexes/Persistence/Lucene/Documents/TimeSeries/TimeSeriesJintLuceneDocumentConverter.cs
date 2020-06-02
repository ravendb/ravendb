using Raven.Client;
using Raven.Server.Documents.Indexes.Static.TimeSeries;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.TimeSeries
{
    public sealed class TimeSeriesJintLuceneDocumentConverter : JintLuceneDocumentConverterBase
    {
        public TimeSeriesJintLuceneDocumentConverter(MapTimeSeriesIndex index)
            : base(index, index.Definition.IndexDefinition, numberOfBaseFields: 3, keyFieldName: Constants.Documents.Indexing.Fields.DocumentIdFieldName, storeValue: true, storeValueFieldName: Constants.Documents.Indexing.Fields.ValueFieldName)
        {
        }

        public TimeSeriesJintLuceneDocumentConverter(MapReduceTimeSeriesIndex index)
            : base(index, index.Definition.IndexDefinition, numberOfBaseFields: 3, keyFieldName: Constants.Documents.Indexing.Fields.DocumentIdFieldName, storeValue: true, storeValueFieldName: Constants.Documents.Indexing.Fields.ValueFieldName)
        {
        }
    }
}
