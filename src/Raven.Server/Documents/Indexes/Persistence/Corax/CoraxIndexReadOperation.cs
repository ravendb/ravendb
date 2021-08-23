using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Corax;
using Corax.Queries;
using Org.BouncyCastle.Crypto;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.Persistence.Corax
{
    public class CoraxIndexReadOperation : IndexReadOperationBase
    {
        private readonly IndexSearcher _indexSearcher;
        private readonly CoraxQueryEvaluator _coraxQueryEvaluator;
        private long _entriesCount = 0;
        public CoraxIndexReadOperation(Index index, Logger logger, Transaction readTransaction) : base(index, logger)
        {
            _indexSearcher = new IndexSearcher(readTransaction);
            _coraxQueryEvaluator = new CoraxQueryEvaluator(_indexSearcher);
        }

        public override void Dispose()
        {
            _indexSearcher?.Dispose();
        }

        public override long EntriesCount() => _entriesCount;

        public override IEnumerable<QueryResult> Query(IndexQueryServerSide query, QueryTimingsScope queryTimings, FieldsToFetch fieldsToFetch,
            Reference<int> totalResults, Reference<int> skippedResults,
            IQueryResultRetriever retriever, DocumentsOperationContext documentsContext, Func<string, SpatialField> getSpatialField, CancellationToken token)
        {
            var pageSize = query.PageSize;
            var isDistinctCount = pageSize == 0 && query.Metadata.IsDistinct;
            if (isDistinctCount)
                pageSize = int.MaxValue;
            var docsToGet = pageSize;
            var position = query.Start;


            IQueryMatch result = _coraxQueryEvaluator.Search(query.Metadata.Query);
            totalResults.Value = Convert.ToInt32(result.Count);

            var retrieverInstance = retriever as MapQueryResultRetriever;
            if (retrieverInstance == null)
                throw new Exception("Wrong instance!");

            List<string> ids = GetIds(ref result, position, docsToGet, token);
            var limit = ids.Count <= position + pageSize ? ids.Count : position + pageSize;

            for (int i = position; i < limit; ++i)
            {
                yield return new QueryResult()
                {
                    Result = retrieverInstance.GetDocumentById(ids[i])

                };
            }
        }


        //todo maciej: optimize it in future.
        private List<string> GetIds(ref IQueryMatch result, long skip, int pageSize, CancellationToken token)
        {
            List<string> list = new();
            Span<long> ids = stackalloc long[16];
            int read;
            long alreadyLoaded = 0;
            var limit = skip + pageSize;
            do {
                token.ThrowIfCancellationRequested();
                read = result.Fill(ids);
                for (int i = 0; i < read; i++)
                {
                    token.ThrowIfCancellationRequested();
                    list.Add(_indexSearcher.GetIdentityFor(ids[i]));
                    alreadyLoaded++;
                }
            } while (alreadyLoaded < limit && read != 0);

            return list;
        }

        public override IEnumerable<QueryResult> IntersectQuery(IndexQueryServerSide query, FieldsToFetch fieldsToFetch, Reference<int> totalResults, Reference<int> skippedResults, IQueryResultRetriever retriever,
            DocumentsOperationContext documentsContext, Func<string, SpatialField> getSpatialField, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public override HashSet<string> Terms(string field, string fromValue, long pageSize, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<QueryResult> MoreLikeThis(IndexQueryServerSide query, IQueryResultRetriever retriever, DocumentsOperationContext context, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<BlittableJsonReaderObject> IndexEntries(IndexQueryServerSide query, Reference<int> totalResults, DocumentsOperationContext documentsContext, Func<string, SpatialField> getSpatialField, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<string> DynamicEntriesFields(HashSet<string> staticFields)
        {
            throw new NotImplementedException();
        }
    }
}
