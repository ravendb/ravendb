using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries.Timings;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries.Results.TimeSeries
{
    public class TimeSeriesQueryResultRetriever : StoredValueQueryResultRetriever
    {
        public TimeSeriesQueryResultRetriever(DocumentDatabase database, IndexQueryServerSide query, QueryTimingsScope queryTimings, DocumentsStorage documentsStorage, JsonOperationContext context, SearchEngineType searchEngineType, FieldsToFetch fieldsToFetch, IncludeDocumentsCommand includeDocumentsCommand, IncludeCompareExchangeValuesCommand includeCompareExchangeValuesCommand, IncludeRevisionsCommand includeRevisionsCommand) 
            : base(Constants.Documents.Indexing.Fields.ValueFieldName, database, query, queryTimings, documentsStorage, context, searchEngineType, fieldsToFetch, includeDocumentsCommand, includeCompareExchangeValuesCommand, includeRevisionsCommand)
        {
        }
    }
}
