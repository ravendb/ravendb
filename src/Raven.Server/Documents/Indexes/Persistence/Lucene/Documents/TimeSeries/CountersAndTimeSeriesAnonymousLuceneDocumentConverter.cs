using Raven.Client;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.TimeSeries
{
    public sealed class CountersAndTimeSeriesAnonymousLuceneDocumentConverter : AnonymousLuceneDocumentConverterBase
    {
        public CountersAndTimeSeriesAnonymousLuceneDocumentConverter(Index index)
            : base(index, numberOfBaseFields: 3, keyFieldName: Constants.Documents.Indexing.Fields.DocumentIdFieldName, storeValue: true, storeValueFieldName: Constants.Documents.Indexing.Fields.ValueFieldName)
        {
        }
    }
}
