using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using Corax;
using Corax.Pipeline;
using Corax.Queries;
using Raven.Client.Documents.Queries.Explanation;
using Raven.Client.Documents.Queries.Highlighting;
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
using Sparrow.Server;
using Voron.Impl;
using CoraxConstants = Corax.Constants;

namespace Raven.Server.Documents.Indexes.Persistence.Corax
{
    public class CoraxIndexReadOperation : IndexReadOperationBase
    {
        private readonly IndexFieldsMapping _fieldMappings;
        private readonly IndexSearcher _indexSearcher;
        private readonly CoraxQueryEvaluator _coraxQueryEvaluator;
        private readonly ByteStringContext _allocator;
        private long _entriesCount = 0;
        private const int BufferSize = 4096;

        public CoraxIndexReadOperation(Index index, Logger logger, Transaction readTransaction) : base(index, logger)
        {
            _allocator = readTransaction.Allocator;
            _fieldMappings = CoraxDocumentConverterBase.GetKnownFields(_allocator, index);
            _fieldMappings.UpdateAnalyzersInBindings(CoraxIndexingHelpers.CreateCoraxAnalyzers(_allocator, index, index.Definition, true));
            _indexSearcher = new IndexSearcher(readTransaction, _fieldMappings);
            _coraxQueryEvaluator = new CoraxQueryEvaluator(index, _indexSearcher);
        }

        public override long EntriesCount() => _entriesCount;


        public override IEnumerable<QueryResult> Query(IndexQueryServerSide query, QueryTimingsScope queryTimings, FieldsToFetch fieldsToFetch,
            Reference<int> totalResults, Reference<int> skippedResults,
            Reference<int> scannedDocuments, IQueryResultRetriever retriever, DocumentsOperationContext documentsContext, Func<string, SpatialField> getSpatialField,
            CancellationToken token)
        {
            var pageSize = query.PageSize;
            var isDistinctCount = pageSize == 0 && query.Metadata.IsDistinct;
            if (isDistinctCount)
                pageSize = int.MaxValue;
            var position = query.Start;

            var take = pageSize + position;
            if (take > _indexSearcher.NumberOfEntries || fieldsToFetch.IsDistinct)
                take = CoraxConstants.IndexSearcher.TakeAll;

            IQueryMatch queryMatch;
            if ((queryMatch = _coraxQueryEvaluator.Search(query, fieldsToFetch, take: take)) is null)
                yield break;

            if (query.Metadata.FilterScript != null)
            {
                throw new NotSupportedException(
                    "Filter isn't supported by Corax. We need to extract the filter feature implementation so it won't be implemented inside the read operations");
            }

            var longIds = ArrayPool<long>.Shared.Rent(CoraxGetPageSize(_indexSearcher, BufferSize, query));
            Span<long> ids = longIds;
            int docsToLoad = pageSize;
            using var queryScope = new CoraxIndexQueryingScope(_index.Type, query, fieldsToFetch, retriever, _indexSearcher, _fieldMappings);
            int queryStart = query.Start;

            while (true)
            {
                token.ThrowIfCancellationRequested();
                int i = queryScope.RecordAlreadyPagedItemsInPreviousPage(longIds.AsSpan(), queryMatch, totalResults, out var read, ref queryStart, token);
                for (; docsToLoad != 0 && i < read; ++i, --docsToLoad)
                {
                    token.ThrowIfCancellationRequested();
                    if (queryScope.WillProbablyIncludeInResults(_indexSearcher.GetRawIdentityFor(longIds[i])) == false)
                    {
                        docsToLoad++;
                        skippedResults.Value++;
                        continue;
                    }

                    var retrieverInput = new RetrieverInput(_fieldMappings, _indexSearcher.GetReaderAndIdentifyFor(longIds[i], out var key), key);
                    bool markedAsSkipped = false;
                    var fetchedDocument = retriever.Get(ref retrieverInput, token);

                    if (fetchedDocument.Document != null)
                    {
                        var qr = GetQueryResult(fetchedDocument.Document, ref markedAsSkipped);
                        if (qr.Result is null)
                        {
                            docsToLoad++;
                            continue;
                        }

                        yield return qr;
                    }
                    else if (fetchedDocument.List != null)
                    {
                        foreach (Document item in fetchedDocument.List)
                        {
                            var qr = GetQueryResult(item, ref markedAsSkipped);
                            if (qr.Result is null)
                            {
                                docsToLoad++;
                                continue;
                            }

                            yield return qr;
                        }
                    }
                }

                if ((read = queryMatch.Fill(longIds)) == 0)
                    break;
                totalResults.Value += read;
            }

            if (isDistinctCount)
                totalResults.Value -= skippedResults.Value;

            QueryResult GetQueryResult(Document document, ref bool markedAsSkipped)
            {
                if (queryScope.TryIncludeInResults(document) == false)
                {
                    document?.Dispose();

                    if (markedAsSkipped == false)
                    {
                        skippedResults.Value++;
                        markedAsSkipped = true;
                    }

                    return default;
                }

                if (isDistinctCount == false)
                {
                    if (query.Metadata.HasHighlightings)
                    {
                        throw new NotImplementedException($"{nameof(Corax)} doesn't support {nameof(Highlightings)} yet.");
                    }

                    if (query.Metadata.HasExplanations)
                    {
                        throw new NotImplementedException($"{nameof(Corax)} doesn't support {nameof(Explanations)} yet.");
                    }

                    return new QueryResult {Result = document, Highlightings = null, Explanation = null};
                }

                return default;
            }

            ArrayPool<long>.Shared.Return(longIds);
        }

        public override IEnumerable<QueryResult> IntersectQuery(IndexQueryServerSide query, FieldsToFetch fieldsToFetch, Reference<int> totalResults,
            Reference<int> skippedResults, Reference<int> scannedDocuments, IQueryResultRetriever retriever,
            DocumentsOperationContext documentsContext, Func<string, SpatialField> getSpatialField, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public override HashSet<string> Terms(string field, string fromValue, long pageSize, CancellationToken token)
        {
            HashSet<string> results = new();
            var terms = _indexSearcher.GetTermsOfField(field);

            if (fromValue is not null)
            {
                Span<byte> fromValueBytes = Encodings.Utf8.GetBytes(fromValue);
                while (terms.GetNextTerm(out var termSlice))
                {
                    token.ThrowIfCancellationRequested();
                    if (termSlice.SequenceEqual(fromValueBytes))
                        break;
                }
            }

            while (pageSize > 0 && terms.GetNextTerm(out var termSlice))
            {
                token.ThrowIfCancellationRequested();
                results.Add(Encodings.Utf8.GetString(termSlice));
                pageSize--;
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
            var position = query.Start;

            if (query.Metadata.IsDistinct)
                throw new NotSupportedException("We don't support Distinct in \"Show Raw Entry\" of Index.");

            var take = pageSize + position;
            if (take > _indexSearcher.NumberOfEntries)
                take = CoraxConstants.IndexSearcher.TakeAll;

            IQueryMatch queryMatch;
            if ((queryMatch = _coraxQueryEvaluator.Search(query, indexFieldsMapping: _fieldMappings, take: take)) is null)
                yield break;

            if (query.Metadata.FilterScript != null)
            {
                throw new NotSupportedException(
                    "Filter isn't supported by Corax. We need to extract the filter feature implementation so it won't be implemented inside the read operations");
            }

            var longIds = ArrayPool<long>.Shared.Rent(CoraxGetPageSize(_indexSearcher, BufferSize, query));

            List<string> itemList = new(32);
            var bufferSizes = GetMaximumSizeOfBuffer();
            var tokensBuffer = ArrayPool<Token>.Shared.Rent(bufferSizes.TokenSize);
            var encodedBuffer = ArrayPool<byte>.Shared.Rent(bufferSizes.OutputSize);
            
            int docsToLoad = pageSize;

            int read;
            int i = Skip();
            while (true)
            {
                token.ThrowIfCancellationRequested();
                for (; docsToLoad != 0 && i < read; ++i, --docsToLoad)
                {
                    token.ThrowIfCancellationRequested();
                    var reader = _indexSearcher.GetReaderAndIdentifyFor(longIds[i], out var id);
                    yield return documentsContext.ReadObject(GetRawDocument(reader), id);
                }

                if ((read = queryMatch.Fill(longIds)) == 0)
                    break;
                totalResults.Value += read;
            }
            
            ArrayPool<long>.Shared.Return(longIds);
            ArrayPool<byte>.Shared.Return(encodedBuffer);
            ArrayPool<Token>.Shared.Return(tokensBuffer);

            DynamicJsonValue GetRawDocument(in IndexEntryReader reader)
            {
                var doc = new DynamicJsonValue();
                foreach (var binding in _fieldMappings)
                {
                    if (binding.FieldIndexingMode is FieldIndexingMode.No || binding.FieldNameAsString is Client.Constants.Documents.Indexing.Fields.AllStoredFields)
                        continue;
                    var type = reader.GetFieldType(binding.FieldId, out _);
                    if ((type & IndexEntryFieldType.List) != 0)
                    {
                        reader.TryReadMany(binding.FieldId, out var iterator);
                        var enumerableEntries = new List<object>();
                        while (iterator.ReadNext())
                        {
                            if (binding.FieldIndexingMode is FieldIndexingMode.Exact)
                            {
                                enumerableEntries.Add(Encodings.Utf8.GetString(iterator.Sequence));
                                continue;
                            }

                            enumerableEntries.Add(GetAnalyzedItem(binding, iterator.Sequence));
                        }
                        
                        doc[binding.FieldNameAsString] = enumerableEntries.ToArray();
                    }
                    else
                    {
                        reader.Read(binding.FieldId, out Span<byte> value);
                        if (binding.FieldIndexingMode is FieldIndexingMode.Exact)
                        {
                            doc[binding.FieldNameAsString] = Encodings.Utf8.GetString(value);
                            continue;
                        }

                        doc[binding.FieldNameAsString] = GetAnalyzedItem(binding, value);
                    }
                }

                return doc;
            }

            object GetAnalyzedItem(IndexFieldBinding binding, ReadOnlySpan<byte> value)
            {
                var tokens = tokensBuffer.AsSpan();
                var encoded = encodedBuffer.AsSpan();
                itemList?.Clear();
                binding.Analyzer.Execute(value, ref encoded, ref tokens);
                for (var index = 0; index < tokens.Length; ++index)
                {
                    token.ThrowIfCancellationRequested();
                    itemList.Add(Encodings.Utf8.GetString(encoded.Slice(tokens[index].Offset, (int)tokens[index].Length)));
                }

                return itemList.Count switch
                {
                    1 => itemList[0],
                    > 1 => itemList.ToArray(),
                    _ => string.Empty
                };
            }
            
            (int OutputSize, int TokenSize) GetMaximumSizeOfBuffer()
            {
                int outputSize = 512;
                int tokenSize = 512;
                foreach (var binding in _fieldMappings)
                {
                    token.ThrowIfCancellationRequested();
                    if (binding.Analyzer is null)
                        continue;

                    binding.Analyzer.GetOutputBuffersSize(512, out int tempOutputSize, out int tempTokenSize);
                    tokenSize = Math.Max(tempTokenSize, tokenSize);
                    outputSize = Math.Max(tempOutputSize, outputSize);
                }

                return (outputSize, tokenSize);
            }


            int Skip()
            {
                while (true)
                {
                    token.ThrowIfCancellationRequested();
                    read = queryMatch.Fill(longIds);
                    totalResults.Value += read;
                    
                    if (position > read)
                    {
                        position -= read;
                        continue;
                    }

                    if (position == read)
                    {
                        read = queryMatch.Fill(longIds);
                        totalResults.Value += read;
                        return 0;
                    }

                    return position;
                }                
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

        public override void Dispose()
        {
            var exceptionAggregator = new ExceptionAggregator($"Could not dispose {nameof(CoraxIndexReadOperation)} of {_index.Name}");
            exceptionAggregator.Execute(() => _indexSearcher?.Dispose());
            exceptionAggregator.ThrowIfNeeded();
        }
    }
}
