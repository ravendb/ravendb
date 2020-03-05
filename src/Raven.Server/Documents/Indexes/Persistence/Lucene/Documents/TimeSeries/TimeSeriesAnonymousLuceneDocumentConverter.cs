using Raven.Client;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.TimeSeries
{
    public sealed class TimeSeriesAnonymousLuceneDocumentConverter : AnonymousLuceneDocumentConverterBase
    {
        public TimeSeriesAnonymousLuceneDocumentConverter(Index index)
            : base(index, numberOfBaseFields: 3, keyFieldName: Constants.Documents.Indexing.Fields.DocumentIdFieldName, storeValue: true, storeValueFieldName: Constants.Documents.Indexing.Fields.ValueFieldName)
        {
        }
    }
}
