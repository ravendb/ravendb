using Raven.Client;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public class CountersAndTimeSeriesAnonymousCoraxDocumentConverter : AnonymousCoraxDocumentConverterBase
{
    public CountersAndTimeSeriesAnonymousCoraxDocumentConverter(Index index)
        : base(index, numberOfBaseFields: 3, keyFieldName: Constants.Documents.Indexing.Fields.DocumentIdFieldName, storeValue: true, canContainSourceDocumentId: true)
    {
        
    }
}
