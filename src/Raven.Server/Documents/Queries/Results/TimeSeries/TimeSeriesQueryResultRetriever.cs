using Raven.Client;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries.Timings;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries.Results.TimeSeries
{
    public class TimeSeriesQueryResultRetriever : StoredValueQueryResultRetriever
    {
        public TimeSeriesQueryResultRetriever(DocumentDatabase database, IndexQueryServerSide query, QueryTimingsScope queryTimings, DocumentsStorage documentsStorage, JsonOperationContext context, FieldsToFetch fieldsToFetch, IncludeDocumentsCommand includeDocumentsCommand, IncludeCompareExchangeValuesCommand includeCompareExchangeValuesCommand) 
            : base(Constants.Documents.Indexing.Fields.ValueFieldName, database, query, queryTimings, documentsStorage, context, fieldsToFetch, includeDocumentsCommand, includeCompareExchangeValuesCommand)
        {
        }
    }
}
