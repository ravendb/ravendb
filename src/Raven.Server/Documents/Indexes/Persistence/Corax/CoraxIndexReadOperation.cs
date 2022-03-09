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
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Voron.Impl;
using Constants = Raven.Client.Constants;
using CoraxConstants = Corax.Constants;
namespace Raven.Server.Documents.Indexes.Persistence.Corax
{
    public class CoraxIndexReadOperation : IndexReadOperationBase
    {
        private readonly IndexFieldsMapping _fieldMappings;
        private readonly IndexSearcher _indexSearcher;
        private readonly CoraxQueryEvaluator _coraxQueryEvaluator;
        private long _entriesCount = 0;
        private const int BufferSize = 4096;

        public CoraxIndexReadOperation(Index index, Logger logger, Transaction readTransaction) : base(index, logger)
        {
            _fieldMappings = CoraxDocumentConverterBase.GetKnownFields(readTransaction.Allocator, index);
            _fieldMappings.UpdateAnalyzersInBindings(CoraxIndexingHelpers.CreateCoraxAnalyzers(readTransaction.Allocator, index, index.Definition, true));
            _indexSearcher = new IndexSearcher(readTransaction, _fieldMappings);
            _coraxQueryEvaluator = new CoraxQueryEvaluator(index, _indexSearcher);
        }

        public override long EntriesCount() => _entriesCount;

        public override IEnumerable<QueryResult> Query(IndexQueryServerSide query, QueryTimingsScope queryTimings, FieldsToFetch fieldsToFetch,
            Reference<int> totalResults, Reference<int> skippedResults,
            Reference<int> scannedDocuments, IQueryResultRetriever retriever, DocumentsOperationContext documentsContext, Func<string, SpatialField> getSpatialField, CancellationToken token)
        {
            var pageSize = query.PageSize;
            var isDistinctCount = pageSize == 0 && query.Metadata.IsDistinct;
            if (isDistinctCount)
                pageSize = int.MaxValue;
            var position = query.Start;

            var take = pageSize + position;
            if (take > _indexSearcher.NumberOfEntries)
                take = CoraxConstants.IndexSearcher.TakeAll;

            IQueryMatch result = _coraxQueryEvaluator.Search(query, fieldsToFetch, take);
            if (result is null)
                yield break;

            if (query.Metadata.FilterScript != null)
            {
                throw new NotSupportedException(
                    "Filter isn't supported by Corax. We need to extract the filter feature implementation so it won't be implemented inside the read operations");
            }

            var ids = ArrayPool<long>.Shared.Rent(CoraxGetPageSize(_indexSearcher, BufferSize));

            int read = 0;
            int docsToLoad = pageSize;
            Skip(ref result, position, ref read, skippedResults, out var readCounter, ref ids, token);

            while (read != 0)
            {
                for (int i = 0; i < read && docsToLoad != 0; --docsToLoad, ++i)
                {
                    RetrieverInput retrieverInput = new(_fieldMappings, _indexSearcher.GetReaderFor(ids[i]), _indexSearcher.GetIdentityFor(ids[i]));
                    var fetchedDocument = retriever.Get(ref retrieverInput, token);

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

            ArrayPool<long>.Shared.Return(ids);
        }

        public override IEnumerable<QueryResult> IntersectQuery(IndexQueryServerSide query, FieldsToFetch fieldsToFetch, Reference<int> totalResults,
            Reference<int> skippedResults, Reference<int> scannedDocuments, IQueryResultRetriever retriever,
            DocumentsOperationContext documentsContext, Func<string, SpatialField> getSpatialField, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public override HashSet<string> Terms(string field, string fromValue, long pageSize, CancellationToken token)
        {
            int fieldId = 0;
            HashSet<string> results = new();

            if (field is Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName or Constants.Documents.Indexing.Fields.DocumentIdFieldName == false)
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

            long[] ids = null;
            try
            {
                ids = ArrayPool<long>.Shared.Rent(BufferSize);
                int read = 0;
                var allItems = _indexSearcher.AllEntries();

                int skip = 0;
                if (fromValue != null && int.TryParse(fromValue, out skip) == false)
                    throw new InvalidDataException($"Wrong {fromValue} input. Please change it into integer number.");

                Skip(ref allItems, skip, ref read, null, out _, ref ids, token);

                var analyzer = _fieldMappings.GetByFieldId(fieldId).Analyzer;
                analyzer.GetOutputBuffersSize(512, out int outputSize, out int tokenSize);
                var encodedBuffer = new byte[outputSize];
                var tokensBuffer = new Token[tokenSize];


                while (read != 0 && results.Count < pageSize)
                {
                    token.ThrowIfCancellationRequested();
                    for (int i = 0; i < read; ++i)
                    {
                        Span<byte> encoded = encodedBuffer;
                        Span<Token> tokens = tokensBuffer;
                        token.ThrowIfCancellationRequested();
                        var reader = _indexSearcher.GetReaderFor(ids[i]);
                        reader.Read(fieldId, out Span<byte> value);
                        analyzer.Execute(value, ref encoded, ref tokens);
                        for (int tIndex = 0; tIndex < tokens.Length; ++tIndex)
                        {
                            var result = encoded.Slice(tokens[tIndex].Offset, (int)tokens[tIndex].Length);
                            results.Add(System.Text.Encoding.UTF8.GetString(result));
                        }
                    }

                    read = allItems.Fill(ids);
                }
            }
            finally
            {
                if (ids is not null)
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

            int outputSize = 0;
            int tokenSize = 0;
            foreach (var binding in _fieldMappings)
            {
                var analyzer = binding.Analyzer;
                if (analyzer == null)
                    continue;        
                
                analyzer.GetOutputBuffersSize(512, out int tempOutputSize, out int tempTokenSize);

                if (tempTokenSize > tokenSize)
                    tokenSize = tempTokenSize;
                if (tempOutputSize > outputSize)
                    outputSize = tempOutputSize;
            }

            if (outputSize is 0 || tokenSize is 0)
                throw new InvalidDataException($"Analyzers of {_index.Name} does not exist or are invalid.");

            long[] ids = null;
            byte[] encodedBuffer = null;
            Token[] tokensBuffer = null;

            try
            {
                var names = MapIndexIdentifiers();
                var allDocsInIndex = _coraxQueryEvaluator.Search(query, new FieldsToFetch(query, _index.Definition));
                totalResults.Value = Convert.ToInt32(allDocsInIndex.Count);
                ids = ArrayPool<long>.Shared.Rent(BufferSize);

                var read = 0;
                Skip(ref allDocsInIndex, skip, ref read, new Reference<int>(), out var readCounter, ref ids, token);
                int returnedCounter = 0;
                List<string> listItemInIndex = null;


                encodedBuffer = ArrayPool<byte>.Shared.Rent(outputSize);
                tokensBuffer = ArrayPool<Token>.Shared.Rent(tokenSize);

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

                            var binding = _fieldMappings.GetByFieldId(fieldId);

                            var analyzer = binding.Analyzer;
                            var name = names[fieldId];
                            var reader = _indexSearcher.GetReaderFor(ids[i]);
                            Span<byte> encoded = encodedBuffer;
                            Span<Token> tokens = tokensBuffer;

                            if (reader.TryReadMany(fieldId, out var iterator))
                            {
                                List<string[]> map = new(8);
                                while (iterator.ReadNext())
                                {
                                    token.ThrowIfCancellationRequested();
                                    analyzer.Execute(iterator.Sequence, ref encoded, ref tokens);
                                    listItemInIndex ??= new(32);
                                    listItemInIndex.Clear();
                                    for (var index = 0; index < tokens.Length; index++)
                                    {
                                        token.ThrowIfCancellationRequested();
                                        var t = tokens[index];
                                        listItemInIndex.Add(Encodings.Utf8.GetString(encoded.Slice(t.Offset, (int)t.Length)));
                                    }

                                    map.Add(listItemInIndex.ToArray());
                                }

                                doc[name] = map;
                            }
                            else if (reader.Read(fieldId, out var value))
                            {
                                analyzer.Execute(value, ref encoded, ref tokens);
                                if (tokens.Length > 1)
                                {
                                    listItemInIndex ??= new(32);
                                    listItemInIndex.Clear();
                                    for (var index = 0; index < tokens.Length; index++)
                                    {
                                        token.ThrowIfCancellationRequested();
                                        var t = tokens[index];
                                        listItemInIndex.Add(Encodings.Utf8.GetString(encoded.Slice(t.Offset, (int)t.Length)));
                                    }

                                    doc[name] = listItemInIndex.ToArray();
                                    continue;
                                }

                                doc[name] = Encodings.Utf8.GetString(encoded);
                            }
                        }

                        returnedCounter++;
                        yield return documentsContext.ReadObject(doc, "index/entries");
                    }

                    read = allDocsInIndex.Fill(ids);
                }
            }
            finally
            {
                if (ids is not null)
                    ArrayPool<long>.Shared.Return(ids);
                if (encodedBuffer is not null)
                    ArrayPool<byte>.Shared.Return(encodedBuffer);
                if (tokensBuffer is not null)
                    ArrayPool<Token>.Shared.Return(tokensBuffer);
            }

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
            if (ids.Length is 0)
                throw new OutOfMemoryException("Corax buffer has no memory.");

            readCounter = 0;
            if (position != 0)
            {
                int emptyRead = position / ids.Length;
                while (emptyRead > 0)
                {
                    token.ThrowIfCancellationRequested();
                    read = result.Fill(ids);
                    readCounter += read;
                    emptyRead--;
                }

                position %= ids.Length;

                //We skipped N * BufferSize chunks but there are still items to skip
                if (position > 0)
                {
                    read = result.Fill(ids);
                    readCounter += read;

                    if (read > position)
                    {
                        ids[position..read].CopyTo(ids, 0); // skipping elements
                        read -= position;
                    }
                    else
                        read = 0;
                }
                else
                {
                    read = result.Fill(ids);
                    readCounter += read;
                }

                if (skippedResults is not null)
                    skippedResults.Value = Convert.ToInt32(readCounter);
            }
            else
            {
                read = result.Fill(ids);
                readCounter += read;
            }

            if (skippedResults is not null)
                skippedResults.Value = 0;
        }

        public override void Dispose()
        {
            var exceptionAggregator = new ExceptionAggregator($"Could not dispose {nameof(CoraxIndexReadOperation)} of {_index.Name}");
            exceptionAggregator.Execute(() => _indexSearcher?.Dispose());
            exceptionAggregator.ThrowIfNeeded();
        }
    }
}
