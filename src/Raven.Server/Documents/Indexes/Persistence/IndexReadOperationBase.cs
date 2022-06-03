using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Lucene.Net.Search;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Explanation;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Indexes.Persistence
{
    public abstract class IndexReadOperationBase : IndexOperationBase
    {
        protected readonly QueryBuilderFactories QueryBuilderFactories;

        protected IndexReadOperationBase(Index index, Logger logger, QueryBuilderFactories queryBuilderFactories) : base(index, logger)
        {
            QueryBuilderFactories = queryBuilderFactories;
        }

        public abstract long EntriesCount();

        public abstract IEnumerable<QueryResult> Query(IndexQueryServerSide query, QueryTimingsScope queryTimings, FieldsToFetch fieldsToFetch,
            Reference<int> totalResults, Reference<int> skippedResults, Reference<int> scannedDocuments, IQueryResultRetriever retriever, DocumentsOperationContext documentsContext,
            Func<string, SpatialField> getSpatialField, CancellationToken token);

        public abstract IEnumerable<QueryResult> IntersectQuery(IndexQueryServerSide query, FieldsToFetch fieldsToFetch, Reference<int> totalResults,
            Reference<int> skippedResults, Reference<int> scannedDocuments, IQueryResultRetriever retriever, DocumentsOperationContext documentsContext, Func<string, SpatialField> getSpatialField,
            CancellationToken token);

        public abstract SortedSet<string> Terms(string field, string fromValue, long pageSize, CancellationToken token);

        public abstract IEnumerable<QueryResult> MoreLikeThis(
            IndexQueryServerSide query,
            IQueryResultRetriever retriever,
            DocumentsOperationContext context,
            CancellationToken token);

        public abstract IEnumerable<BlittableJsonReaderObject> IndexEntries(IndexQueryServerSide query, Reference<int> totalResults, DocumentsOperationContext documentsContext,
            Func<string, SpatialField> getSpatialField, bool ignoreLimit, CancellationToken token);

        public abstract IEnumerable<string> DynamicEntriesFields(HashSet<string> staticFields);

        internal static unsafe BlittableJsonReaderObject ParseJsonStringIntoBlittable(string json, JsonOperationContext context)
        {
            var bytes = Encoding.UTF8.GetBytes(json);
            fixed (byte* ptr = bytes)
            {
                var blittableJson = context.ParseBuffer(ptr, bytes.Length, "MoreLikeThis/ExtractTermsFromJson", BlittableJsonDocumentBuilder.UsageMode.None);
                blittableJson.BlittableValidation(); //precaution, needed because this is user input..
                return blittableJson;
            }
        }

        public struct QueryResult
        {
            public Document Result;
            public Dictionary<string, Dictionary<string, string[]>> Highlightings;
            public ExplanationResult Explanation;
        }
    }
}
