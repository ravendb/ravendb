using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Corax;
using Corax.Pipeline;
using Corax.Queries;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Exceptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.Persistence.Corax
{
    public class CoraxIndexReadOperation : IndexReadOperationBase
    {
        private JsonOperationContext _jsonAllocator;
        private readonly CoraxRavenPerFieldAnalyzerWrapper _analyzers;
        private readonly IndexSearcher _indexSearcher;
        private readonly CoraxQueryEvaluator _coraxQueryEvaluator;
        private long _entriesCount = 0;
        private const int BufferSize = 2048;

        public CoraxIndexReadOperation(Index index, Logger logger, Transaction readTransaction) : base(index, logger)
        {
            _analyzers = CreateCoraxAnalyzers(index, index.Definition, true);
            _indexSearcher = new IndexSearcher(readTransaction, _analyzers.Analyzers);
            _coraxQueryEvaluator = new CoraxQueryEvaluator(_indexSearcher);
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
                        var fetchedDocument = retriever.Get(ref retrieverInput);

                        if (fetchedDocument.Document != null)
                        {
                            yield return new QueryResult { Result = fetchedDocument.Document };
                        }
                        else if (fetchedDocument.List != null)
                        {
                            foreach (Document item in fetchedDocument.List)
                            {
                                yield return new QueryResult { Result = item };
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
            var pageSize = query.PageSize;
            var skip = query.Start;
            
            var allDocsInIndex = _indexSearcher.AllEntries();
            totalResults.Value = Convert.ToInt32(allDocsInIndex.Count);
            var names = MapIndexIdentifiers();
            var ids = ArrayPool<long>.Shared.Rent(2048);

            var read = 0;
            Skip(ref allDocsInIndex, skip, ref read, new Reference<int>(), out var readCounter, ref ids, token);
            int returnedCounter = 0;
            List<string> listItemInIndex = null;

            while (read != 0 && returnedCounter < pageSize)
            {
                token.ThrowIfCancellationRequested();
                for (int i = 0; i < read && returnedCounter < pageSize; ++i)
                {
                    token.ThrowIfCancellationRequested();
                    var doc = new DynamicJsonValue();

                    for (int fieldId = 0; fieldId < names.Count; ++fieldId)
                    {
                        token.ThrowIfCancellationRequested();

                        var name = names[fieldId];
                        var reader = _indexSearcher.GetReaderFor(ids[i]);

                        if (reader.Read(fieldId, out var value))
                        {
                            doc[name] = Encodings.Utf8.GetString(value);
                        }
                        else if (reader.TryReadMany(fieldId, out var iterator))
                        {
                            listItemInIndex ??= new(32);
                            listItemInIndex.Clear();

                            while (iterator.ReadNext())
                            {
                                token.ThrowIfCancellationRequested();
                                listItemInIndex.Add(Encodings.Utf8.GetString(iterator.Sequence));
                            }

                            doc[name] = listItemInIndex;
                        }
                    }

                    returnedCounter++;
                    yield return documentsContext.ReadObject(doc, "index/entries");
                }

                read = allDocsInIndex.Fill(ids);
            }


            ArrayPool<long>.Shared.Return(ids);

            Dictionary<int, string> MapIndexIdentifiers()
            {
                var dict = new Dictionary<int, string>();
                var firstName = _index.Type.IsMapReduce()
                    ? Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName
                    : Constants.Documents.Indexing.Fields.DocumentIdFieldName;
                dict.Add(0, firstName);
                foreach (var field in _index.Definition.IndexFields.Values)
                {
                    dict.Add(field.Id, field.Name);
                }

                return dict;
            }
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

        private static void Skip<TQueryMatch>(ref TQueryMatch result, int position, ref int read, Reference<int> skippedResults, out long readCounter, ref long[] ids,
            CancellationToken token)
            where TQueryMatch : IQueryMatch
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

            // first Fill operation needs to be done outside loop because we need to check if there is some data
            if (read == 0)
            {
                read = result.Fill(ids);
                readCounter += read;
            }
        }

        public override void Dispose()
        {
            _indexSearcher?.Dispose();
            _analyzers?.Dispose();
            _jsonAllocator?.Dispose();
        }
    }
}
