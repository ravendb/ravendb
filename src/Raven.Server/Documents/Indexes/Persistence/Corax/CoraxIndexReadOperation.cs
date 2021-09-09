using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using Corax;
using Corax.Queries;
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
        private const int BufferSize = 2048;

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
            var position = query.Start;
            long readCounter = 0;

            IQueryMatch result = _coraxQueryEvaluator.Search(query.Metadata, fieldsToFetch);
            if (result == null)
                yield break;

            totalResults.Value = Convert.ToInt32(result.Count);
            var ids = ArrayPool<long>.Shared.Rent(BufferSize);
            try
            {
                int read = 0;

                //escaping already returned docs.
                // Q: Can Corax have "skip" method inside? It's inefficient to copy values only for skipping.
                int docsToLoad = pageSize;
                if (position != 0)
                {
                    int emptyRead = position / BufferSize;
                    do
                    {
                        read = result.Fill(ids);
                        readCounter += read;
                        emptyRead--;
                    } while (emptyRead > 0);

                    position %= BufferSize; // move into <0;_bufferSize> set.
                    //I know there is a cost of copying this but it's max _bufferSize and make code much simpler.
                    if (position != read)
                        ids[position..read].CopyTo(ids, 0);
                    else
                        ids[0] = ids[position];
                    read -= position;
                    skippedResults.Value = -1; // docsToLoad * _bufferSize + position;
                }

                // first Fill operation would be done outside loop because we need to check if there is already some data read.
                if (read == 0)
                {
                    read = result.Fill(ids);
                    readCounter += read;
                }

                while (read != 0)
                {
                    for (int i = 0; i < read && docsToLoad != 0; --docsToLoad, ++i)
                    {
                        RetrieverInput retrieverInput = new(_indexSearcher.GetReaderFor(ids[i]), _indexSearcher.GetIdentityFor(ids[i]));

                        yield return new QueryResult() { Result = retriever.Get(ref retrieverInput) };
                    }

                    if (docsToLoad == 0)
                        break;

                    read = result.Fill(ids);
                    readCounter += read;
                }

                //special value for studio. We have unbounded set and don't know how much items is in index so it should count it during loading.
                if (totalResults.Value == 0)
                {
                    totalResults.Value = Convert.ToInt32(readCounter);
                }
            }
            finally
            {
                ArrayPool<long>.Shared.Return(ids);
            }
        }

        public override IEnumerable<QueryResult> IntersectQuery(IndexQueryServerSide query, FieldsToFetch fieldsToFetch, Reference<int> totalResults,
            Reference<int> skippedResults, IQueryResultRetriever retriever,
            DocumentsOperationContext documentsContext, Func<string, SpatialField> getSpatialField, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public override HashSet<string> Terms(string field, string fromValue, long pageSize, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<QueryResult> MoreLikeThis(IndexQueryServerSide query, IQueryResultRetriever retriever, DocumentsOperationContext context,
            CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<BlittableJsonReaderObject> IndexEntries(IndexQueryServerSide query, Reference<int> totalResults,
            DocumentsOperationContext documentsContext, Func<string, SpatialField> getSpatialField, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<string> DynamicEntriesFields(HashSet<string> staticFields)
        {
            throw new NotImplementedException();
        }
    }
}
