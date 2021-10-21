using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using Corax;
using Corax.Queries;
using Raven.Client;
using Raven.Client.Documents.Linq;
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

            IQueryMatch result = _coraxQueryEvaluator.Search(query, fieldsToFetch);
            if (result == null)
                yield break;

            var ids = ArrayPool<long>.Shared.Rent(BufferSize);
            try
            {
                int read = 0;
                int docsToLoad = pageSize;
                Skip(ref result, position, ref read, skippedResults, out var readCounter, ref ids, token);
                
                while (read != 0)
                {
                    for (int i = 0; i < read && docsToLoad != 0; --docsToLoad, ++i)
                    {
                        RetrieverInput retrieverInput = new(_indexSearcher.GetReaderFor(ids[i]), _indexSearcher.GetIdentityFor(ids[i]));
                        var r = retriever.Get(ref retrieverInput);

                        if (r.Document != null)
                        {
                            yield return new QueryResult
                            {
                                Result = r.Document
                            };
                        }
                        else if (r.List != null)
                        {
                            foreach (Document item in r.List)
                            {
                                yield return new QueryResult
                                {
                                    Result = item
                                };
                            }
                        }
                    }

                    if (docsToLoad == 0)
                        break;

                    read = result.Fill(ids);
                    readCounter += read;
                }


                if (result is SortingMatch sm)
                    totalResults.Value = Convert.ToInt32(sm.TotalResults);
                else
                {
                    while (read != 0)
                    {
                        read = result.Fill(ids);
                        readCounter += read;
                    }

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
            int fieldId = 0;
            HashSet<string> results = new();

            if ((field is Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName or Constants.Documents.Indexing.Fields.DocumentIdFieldName) == false)
            {
                fieldId = -1;
                foreach (var indexField in _index.Definition.IndexFields.Values)
                {
                    if (indexField.Name == field)
                    {
                        fieldId = indexField.Id;
                    }
                }
                if (fieldId == -1)
                    throw new InvalidDataException($"Cannot find {field} field in {_index.Name}'s terms.");
            }

            var ids = ArrayPool<long>.Shared.Rent(BufferSize);
            try
            {
                int read = 0;
                var allItems = _indexSearcher.AllEntries();

                int skip = 0;
                if (fromValue != null && int.TryParse(fromValue, out skip) == false)
                    throw new InvalidDataException($"Wrong {fromValue} input. Please change it into integer number.");

                Skip(ref allItems, skip, ref read, null, out _, ref ids, token);

                while (read != 0 && results.Count < pageSize)
                {
                    token.ThrowIfCancellationRequested();
                    for (int i = 0; i < read; ++i)
                    {
                        token.ThrowIfCancellationRequested();
                        var reader = _indexSearcher.GetReaderFor(ids[i]);
                        reader.Read(fieldId, out Span<byte> value);
                        results.Add(System.Text.Encoding.UTF8.GetString(value));
                    }

                    read = allItems.Fill(ids);
                }
            }
            finally
            {
                ArrayPool<long>.Shared.Return(ids);
            }

            return results;
        }

        public override IEnumerable<QueryResult> MoreLikeThis(IndexQueryServerSide query, IQueryResultRetriever retriever, DocumentsOperationContext context,
            CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<BlittableJsonReaderObject> IndexEntries(IndexQueryServerSide query, Reference<int> totalResults,
            DocumentsOperationContext documentsContext, Func<string, SpatialField> getSpatialField, bool ignoreLimit, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<string> DynamicEntriesFields(HashSet<string> staticFields)
        {
            foreach (var field in _index.Definition.IndexFields.Values)
            {
                if (staticFields.Contains(field.Name))
                    continue;
                yield return field.Name;
            }
        }


        private void Skip<TQueryMatch>(ref TQueryMatch result, int position, ref int read, Reference<int> skippedResults, out long readCounter, ref long[] ids,
            CancellationToken token) where TQueryMatch : IQueryMatch
        {
            readCounter = 0;
            if (position != 0)
            {
                int emptyRead = position / BufferSize;
                do
                {
                    token.ThrowIfCancellationRequested();
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
                if (skippedResults != null)
                    skippedResults.Value = Convert.ToInt32(readCounter);
            }

            // first Fill operation would be done outside loop because we need to check if there is already some data read.
            if (read == 0)
            {
                read = result.Fill(ids);
                readCounter += read;
            }
        }
    }
}
