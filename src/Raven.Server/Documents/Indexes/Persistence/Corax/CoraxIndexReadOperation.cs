using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Corax;
using Corax.Pipeline;
using Corax.Queries;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Raven.Client.Documents.Queries.Explanation;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Highlightings;
using Raven.Server.Documents.Queries.MoreLikeThis;
using Raven.Server.Documents.Queries.MoreLikeThis.Corax;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server;
using Voron.Impl;
using CoraxConstants = Corax.Constants;
using IndexSearcher = Corax.IndexSearcher;

namespace Raven.Server.Documents.Indexes.Persistence.Corax
{
    public class CoraxIndexReadOperation : IndexReadOperationBase
    {
        private static readonly ArrayPool<long> QueryPool = ArrayPool<long>.Create();
        private readonly IndexFieldsMapping _fieldMappings;
        private readonly IndexSearcher _indexSearcher;
        private readonly ByteStringContext _allocator;
        private long _entriesCount = 0;
        
        public CoraxIndexReadOperation(Index index, Logger logger, Transaction readTransaction, QueryBuilderFactories queryBuilderFactories, IndexFieldsMapping fieldsMapping, IndexQueryServerSide query) : base(index, logger, queryBuilderFactories, query)
        {
            _allocator = readTransaction.Allocator;
            _fieldMappings = fieldsMapping;
            _indexSearcher = new IndexSearcher(readTransaction, _fieldMappings);
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

            QueryTimingsScope coraxScope = null;
            QueryTimingsScope highlightingScope = null;
            if (queryTimings != null)
            {
                coraxScope = queryTimings.For(nameof(QueryTimingsScope.Names.Corax), start: false);
                highlightingScope = query.Metadata.HasHighlightings
                    ? queryTimings.For(nameof(QueryTimingsScope.Names.Highlightings), start: false)
                    : null;
            }

            IQueryMatch queryMatch;
            Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms = query.Metadata.HasHighlightings ? new() : null;
            bool isBinary;
            using (coraxScope?.Start())
            {
                var builderParameters = new CoraxQueryBuilder.Parameters(_indexSearcher, serverContext: null, documentsContext: null, query, _index, query.QueryParameters, QueryBuilderFactories,
                    _fieldMappings, fieldsToFetch, highlightingTerms, take);
                if ((queryMatch = CoraxQueryBuilder.BuildQuery(builderParameters, out isBinary)) is null)
                    yield break;
            }

            
            var ids = QueryPool.Rent(CoraxGetPageSize(_indexSearcher, take, query, isBinary ));
            int docsToLoad = pageSize;
            int queryStart = query.Start;
            bool hasHighlights = query.Metadata.HasHighlightings;
            if (hasHighlights)
            {
                using (highlightingScope?.For(nameof(QueryTimingsScope.Names.Setup)))
                    SetupHighlighter(query, documentsContext, highlightingTerms);
            }


            using var queryScope = new CoraxIndexQueryingScope(_index.Type, query, fieldsToFetch, retriever, _indexSearcher, _fieldMappings);
            using var queryFilter = GetQueryFilter(_index, query, documentsContext, skippedResults, scannedDocuments, retriever, queryTimings);

            while (true)
            {
                token.ThrowIfCancellationRequested();
                int i = queryScope.RecordAlreadyPagedItemsInPreviousPage(ids.AsSpan(), queryMatch, totalResults, out var read, ref queryStart, token);
                for (; docsToLoad != 0 && i < read; ++i, --docsToLoad)
                {
                    token.ThrowIfCancellationRequested();
                    if (queryScope.WillProbablyIncludeInResults(_indexSearcher.GetRawIdentityFor(ids[i])) == false)
                    {
                        docsToLoad++;
                        skippedResults.Value++;
                        continue;
                    }
                    
                    var retrieverInput = new RetrieverInput(_fieldMappings, _indexSearcher.GetReaderAndIdentifyFor(ids[i], out var key), key);

                    var filterResult = queryFilter?.Apply(ref retrieverInput, key);
                    if (filterResult is not null and not FilterResult.Accepted)
                    {
                        docsToLoad++;
                        if (filterResult is FilterResult.Skipped)
                            continue;
                        if (filterResult is FilterResult.LimitReached)
                            break;
                    }

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
                
                
                if ((read = queryMatch.Fill(ids)) == 0)
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

                Dictionary<string, Dictionary<string, string[]>> highlightings = null;

                if (isDistinctCount == false)
                {
                    if (query.Metadata.HasHighlightings)
                    {
                        using (highlightingScope?.For(nameof(QueryTimingsScope.Names.Fill)))
                        {
                            highlightings = new();

                            // If we have highlightings then we need to setup the Corax objects that will attach to the evaluator in order
                            // to retrieve the fields and perform the transformations required by Highlightings. 
                            foreach (var current in query.Metadata.Highlightings)
                            {
                                // We get the actual highlight description. 

                                var fieldName = current.Field.Value;
                                if (highlightingTerms.TryGetValue(fieldName, out var fieldDescription) == false)
                                    continue;

                                //We have to get analyzer so dynamic field have priority over normal name
                                // We get the field binding to ensure that we are running the analyzer to find the actual tokens.
                                if (_fieldMappings.TryGetByFieldName(fieldDescription.DynamicFieldName ?? fieldDescription.FieldName, out var fieldBinding) == false)
                                    continue;

                                // We will get the actual tokens dictionary for this field. If it exists we get it immediately, if not we create
                                if (!highlightings.TryGetValue(fieldDescription.FieldName, out var tokensDictionary))
                                {
                                    tokensDictionary = new(StringComparer.OrdinalIgnoreCase);
                                    highlightings[fieldDescription.FieldName] = tokensDictionary;
                                }

                                List<string> fragments = new();

                                // We need to get the actual field, not the dynamic field. 
                                int propIdx = document.Data.GetPropertyIndex(fieldDescription.FieldName);
                                BlittableJsonReaderObject.PropertyDetails property = default;
                                document.Data.GetPropertyByIndex(propIdx, ref property);

                                if (property.Token == BlittableJsonToken.String)
                                {
                                    var fieldValue = ((LazyStringValue)property.Value).ToString();
                                    ProcessHighlightings(current, fieldDescription, fieldValue, fragments, current.FragmentCount);
                                }
                                else if (property.Token == BlittableJsonToken.CompressedString)
                                {
                                    var fieldValue = ((LazyCompressedStringValue)property.Value).ToString();
                                    ProcessHighlightings(current, fieldDescription, fieldValue, fragments, current.FragmentCount);
                                }
                                else if ((property.Token & ~BlittableJsonToken.PositionMask) == BlittableJsonToken.StartArray)
                                {
                                    // This is an array, now we need to know if it is compressed or not. 
                                    int maxFragments = current.FragmentCount;
                                    foreach (var item in ((BlittableJsonReaderArray)property.Value).Items)
                                    {
                                        var fieldValue = item.ToString();
                                        maxFragments -= ProcessHighlightings(current, fieldDescription, fieldValue, fragments, maxFragments);
                                    }
                                }
                                else continue;

                                if (fragments.Count > 0)
                                {
                                    string key;
                                    if (string.IsNullOrWhiteSpace(fieldDescription.GroupKey) == false)
                                    {
                                        int groupKey;
                                        if ((groupKey = document.Data.GetPropertyIndex(fieldDescription.GroupKey)) != -1)
                                        {
                                            document.Data.GetPropertyByIndex(groupKey, ref property);

                                            key = property.Token switch
                                            {
                                                BlittableJsonToken.String => ((LazyStringValue)property.Value).ToString(),
                                                BlittableJsonToken.CompressedString => ((LazyCompressedStringValue)property.Value).ToString(),
                                                _ => throw new NotSupportedException($"The token type '{property.Token.ToString()}' is not supported.")
                                            };
                                        }
                                        else
                                        {
                                            key = document.Id;
                                        }
                                    }
                                    else
                                        key = document.Id;

                                    
                                    if (tokensDictionary.TryGetValue(key, out var existingHighlights))
                                        throw new NotSupportedException("Multiple highlightings for the same field and group key are not supported.");

                                    tokensDictionary[key] = fragments.ToArray();

                                }
                            }
                        }
                    }

                    if (query.Metadata.HasExplanations)
                    {
                        throw new NotImplementedException($"{nameof(Corax)} doesn't support {nameof(Explanations)} yet.");
                    }

                    return new QueryResult {Result = document, Highlightings = highlightings, Explanation = null};
                }

                return default;
            }

            QueryPool.Return(ids);
        }

        private static int ProcessHighlightings(HighlightingField current, CoraxHighlightingTermIndex highlightingTerm, ReadOnlySpan<char> fieldFragment, List<string> fragments, int maxFragmentCount)
        {
            int totalFragments = 0;

            // For each potential token we are looking for, and for each token that we need to find... we will test every analyzed token
            // and decide if we create a highlightings fragment for it or not.
            string[] values = (string[])highlightingTerm.Values;
            for (int i = 0; i < values.Length; i++)
            {
                // We have reached the amount of fragments we required.
                if (totalFragments >= maxFragmentCount)
                    break;

                var value = values[i];
                var preTag = highlightingTerm.GetPreTagByIndex(i);
                var postTag = highlightingTerm.GetPostTagByIndex(i);

                int currentIndex = 0;
                while (true)
                {
                    // We have reached the amount of fragments we required.
                    if (totalFragments >= maxFragmentCount)
                        break;

                    // We found an exact match in the property value.
                    var index = fieldFragment.Slice(currentIndex)
                        .IndexOf(value, StringComparison.InvariantCultureIgnoreCase);
                    if (index < 0)
                        break;

                    index += currentIndex; // Adjusting to absolute positioning

                    // We will look for a whitespace before the match to start the token. 
                    int tokenStart = fieldFragment.Slice(0, index)
                        .LastIndexOf(' ');
                    if (tokenStart < 0)
                        tokenStart = 0;

                    // We will look for a whitespace after the match to end the token. 
                    int tokenEnd = fieldFragment.Slice(index)
                        .IndexOf(' ');
                    if (tokenEnd < 0)
                        tokenEnd = fieldFragment.Length - index;

                    tokenEnd += index; // Adjusting to absolute positioning

                    int highlightingLength = tokenEnd - tokenStart;                    
                    int fragmentRestLength = Math.Min(current.FragmentLength - highlightingLength, fieldFragment.Length);
                    if (fragmentRestLength < 0)
                    {
                        fragmentRestLength = 0;
                    }                        
                    else if (fragmentRestLength != fieldFragment.Length)
                    {
                        // We may have a fragment we can find a space near the end. 
                        int fragmentEnd = fieldFragment.Slice(tokenEnd)
                            .LastIndexOf(' ');

                        // We need to discard the space used to separate the token itself.
                        if (fragmentEnd > 0)
                            fragmentRestLength = tokenEnd + fragmentEnd;
                    }

                    var fragmentRest = fragmentRestLength != 0 ? fieldFragment[tokenEnd..fragmentRestLength] : string.Empty;
                    var fragment = $"{preTag}{fieldFragment[tokenStart..tokenEnd]}{postTag}{fragmentRest}";
                    fragments.Add(fragment);

                    totalFragments++;
                    currentIndex = tokenEnd;
                }
            }

            return totalFragments;
        }

        private void SetupHighlighter(IndexQueryServerSide query, JsonOperationContext context, Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms)
        {
            foreach(var term in highlightingTerms)
            {
                string[] nls;
                switch (term.Value.Values)
                {
                    case string s:
                        nls = new string[] { s.TrimEnd('*').TrimStart('*') };
                        break;
                    case List<string> ls:
                        nls = new string[ls.Count];
                        for (int i = 0; i < ls.Count; i++)
                            nls[i] = ls[i].TrimEnd('*').TrimStart('*');
                        break;
                    case Tuple<string, string> t2:
                        nls = new string[] { t2.Item1.TrimEnd('*').TrimStart('*'), t2.Item2.TrimEnd('*').TrimStart('*') };
                        break;
                    case string[] as1:
                        continue;
                    default:
                        throw new NotSupportedException($"The type '{term.Value.Values.GetType().FullName}' is not supported.");
                }
                
                term.Value.Values = nls;
                term.Value.PreTags = null;
                term.Value.PostTags = null;
            }

            foreach (var highlighting in query.Metadata.Highlightings)
            {
                var options = highlighting.GetOptions(context, query.QueryParameters);
                if (options == null)
                    continue;

                var numberOfPreTags = options.PreTags?.Length ?? 0;
                var numberOfPostTags = options.PostTags?.Length ?? 0;
                if (numberOfPreTags != numberOfPostTags)
                    throw new InvalidOperationException("Number of pre-tags and post-tags must match.");

                var fieldName = 
                    query.Metadata.IsDynamic 
                        ? AutoIndexField.GetHighlightingAutoIndexFieldName(highlighting.Field.Value)
                        : highlighting.Field.Value;
                
                if (highlightingTerms.TryGetValue(fieldName, out var termIndex) == false)
                {
                    // the case when we have to create MapReduce highlighter
                    termIndex = new();
                    termIndex.FieldName = highlighting.Field.Value;
                    termIndex.DynamicFieldName = AutoIndexField.GetHighlightingAutoIndexFieldName(highlighting.Field.Value);
                    termIndex.GroupKey = options.GroupKey;
                    highlightingTerms.Add(query.Metadata.IsDynamic ? termIndex.DynamicFieldName :  termIndex.FieldName, termIndex);
                }

                if (termIndex is not null)
                    termIndex.GroupKey = options.GroupKey;
                else
                    continue;
                
                if (numberOfPreTags > 0)
                {
                    termIndex.PreTags = options.PreTags;
                    termIndex.PostTags = options.PostTags;
                }
            }
        }

        public override IEnumerable<QueryResult> IntersectQuery(IndexQueryServerSide query, FieldsToFetch fieldsToFetch, Reference<int> totalResults,
            Reference<int> skippedResults, Reference<int> scannedDocuments, IQueryResultRetriever retriever,
            DocumentsOperationContext documentsContext, Func<string, SpatialField> getSpatialField, CancellationToken token)
        {
            throw new NotImplementedException($"{nameof(Corax)} does not support intersect queries.");
        }

        public override SortedSet<string> Terms(string field, string fromValue, long pageSize, CancellationToken token)
        {
            SortedSet<string> results = new();
            
            if (_indexSearcher.TryGetTermsOfField(field, out var terms) == false)
                return results;
            
            if (string.IsNullOrEmpty(fromValue) == false)
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
            IDisposable releaseServerContext = null;
            IDisposable closeServerTransaction = null;
            TransactionOperationContext serverContext = null;
            MoreLikeThisQuery moreLikeThisQuery;
            var isBinary = false;
            CoraxQueryBuilder.Parameters builderParameters;

            try
            {
                if (query.Metadata.HasCmpXchg)
                {
                    releaseServerContext = context.DocumentDatabase.ServerStore.ContextPool.AllocateOperationContext(out serverContext);
                    closeServerTransaction = serverContext.OpenReadTransaction();
                }

                using (closeServerTransaction)
                {
                    builderParameters = new (_indexSearcher, serverContext, context, query, _index, query.QueryParameters, QueryBuilderFactories,
                        _fieldMappings, null, null /* allow highlighting? */, CoraxQueryBuilder.TakeAll, null);
                    moreLikeThisQuery = CoraxQueryBuilder.BuildMoreLikeThisQuery(builderParameters, query.Metadata.Query.Where, out isBinary);
                }
            }
            finally
            {
                releaseServerContext?.Dispose();
            }

            var options = moreLikeThisQuery.Options != null ? JsonDeserializationServer.MoreLikeThisOptions(moreLikeThisQuery.Options) : MoreLikeThisOptions.Default;

            HashSet<string> stopWords = null;
            if (string.IsNullOrWhiteSpace(options.StopWordsDocumentId) == false)
            {
                var stopWordsDoc = context.DocumentDatabase.DocumentsStorage.Get(context, options.StopWordsDocumentId);
                if (stopWordsDoc == null)
                    throw new InvalidOperationException($"Stop words document {options.StopWordsDocumentId} could not be found");

                if (stopWordsDoc.Data.TryGet(nameof(MoreLikeThisStopWords.StopWords), out BlittableJsonReaderArray value) && value != null)
                {
                    stopWords = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    for (var i = 0; i < value.Length; i++)
                        stopWords.Add(value.GetStringByIndex(i));
                }
            }

            builderParameters = new (_indexSearcher, null, context, query, _index, query.QueryParameters, QueryBuilderFactories,
                _fieldMappings, null, null /* allow highlighting? */, CoraxQueryBuilder.TakeAll, null);
            var mlt = new RavenRavenMoreLikeThis(builderParameters, options);
            long? baseDocId = null;

            if (moreLikeThisQuery.BaseDocument == null)
            {

                Span<long> docsIds = stackalloc long[16];


                // get the current Lucene docid for the given RavenDB doc ID
                if (moreLikeThisQuery.BaseDocumentQuery.Fill(docsIds) == 0)
                    throw new InvalidOperationException("Given filtering expression did not yield any documents that could be used as a base of comparison");
                
                //What if we've got multiple items?
                baseDocId = docsIds[0];
            }

            if (stopWords != null)
                mlt.SetStopWords(stopWords);

            string[] fieldNames;
            if (options.Fields != null && options.Fields.Length > 0)
                fieldNames = options.Fields;
            else
            {
                fieldNames = new string[_fieldMappings.Count];
                var index = 0;
                foreach (var binding in _fieldMappings)
                {
                    if (binding.FieldNameAsString is Client.Constants.Documents.Indexing.Fields.DocumentIdFieldName or Client.Constants.Documents.Indexing.Fields.SourceDocumentIdFieldName or Client.Constants.Documents.Indexing.Fields.ReduceKeyHashFieldName)
                        continue;
                    fieldNames[index++] = binding.FieldNameAsString;

                }

                if (index < fieldNames.Length)
                    Array.Resize(ref fieldNames, index);
            }

            mlt.SetFieldNames(fieldNames);

            var pageSize = CoraxGetPageSize(_indexSearcher, query.PageSize, query, isBinary);

            IQueryMatch mltQuery;
            if (baseDocId.HasValue)
            {
                mltQuery = mlt.Like(baseDocId.Value);
            }
            else
            {
                using (var blittableJson = ParseJsonStringIntoBlittable(moreLikeThisQuery.BaseDocument, context))
                    mltQuery = mlt.Like(blittableJson);
            }

            if (moreLikeThisQuery.FilterQuery != null && moreLikeThisQuery.FilterQuery is AllEntriesMatch == false)
            {
                mltQuery = _indexSearcher.And(mltQuery, moreLikeThisQuery.FilterQuery);
            }
            


            var ravenIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            long[] ids = QueryPool.Rent(pageSize);
            var read = 0;

            while ((read = mltQuery.Fill(ids.AsSpan())) != 0)
            {
                for (int i = 0; i < read; i++)
                {
                    var hit = ids[i];
                    token.ThrowIfCancellationRequested();

                    if (hit == baseDocId)
                        continue;

                    var reader = _indexSearcher.GetReaderAndIdentifyFor(hit, out string id);

                    if (ravenIds.Add(id) == false)
                        continue;

                    var retrieverInput = new RetrieverInput(_fieldMappings, reader, id);
                    var result = retriever.Get(ref retrieverInput, token);
                    if (result.Document != null)
                    {
                        yield return new QueryResult {Result = result.Document};
                    }
                    else if (result.List != null)
                    {
                        foreach (Document item in result.List)
                        {
                            yield return new QueryResult {Result = item};
                        }
                    }
                }
            }

            QueryPool.Return(ids);
        }

        public override IEnumerable<BlittableJsonReaderObject> IndexEntries(IndexQueryServerSide query, Reference<int> totalResults,
            DocumentsOperationContext documentsContext, Func<string, SpatialField> getSpatialField, bool ignoreLimit, CancellationToken token)
        {
            var pageSize = query.PageSize;
            var position = query.Start;

            if (query.Metadata.IsDistinct)
                throw new NotSupportedException("We don't support Distinct in \"Show Raw Entry\" of Index.");
            if (query.Metadata.FilterScript != null)
                throw new NotSupportedException(
                    "Filter isn't supported in Raw Index View.");

            var take = pageSize + position;
            if (take > _indexSearcher.NumberOfEntries)
                take = CoraxConstants.IndexSearcher.TakeAll;

            IQueryMatch queryMatch;
            bool isBinary;
            var builderParameters = new CoraxQueryBuilder.Parameters(_indexSearcher, null, null, query, _index, null, null, _fieldMappings, null, null, -1, null);
            if ((queryMatch = CoraxQueryBuilder.BuildQuery(builderParameters, out isBinary)) is null)
                yield break;

            var ids = QueryPool.Rent(CoraxGetPageSize(_indexSearcher, take, query, isBinary));

            HashSet<string> itemList = new(32);

            var maxTermLengthProceedPerAnalyzer = ArrayPool<int>.Shared.Rent(_fieldMappings.Count);
            var encodedBuffer = Analyzer.BufferPool.Rent(_fieldMappings.MaximumOutputSize);
            var tokensBuffer = Analyzer.TokensPool.Rent(_fieldMappings.MaximumTokenSize);

            int docsToLoad = pageSize;

            int read;
            int i = Skip();
            while (true)
            {
                token.ThrowIfCancellationRequested();
                for (; docsToLoad != 0 && i < read; ++i, --docsToLoad)
                {
                    token.ThrowIfCancellationRequested();
                    var reader = _indexSearcher.GetReaderAndIdentifyFor(ids[i], out var id);
                    yield return documentsContext.ReadObject(GetRawDocument(ref reader), id);
                }

                if ((read = queryMatch.Fill(ids)) == 0)
                    break;
                totalResults.Value += read;
            }

            QueryPool.Return(ids);
            Analyzer.BufferPool.Return(encodedBuffer);
            Analyzer.TokensPool.Return(tokensBuffer);
            ArrayPool<int>.Shared.Return(maxTermLengthProceedPerAnalyzer);
            DynamicJsonValue GetRawDocument(ref IndexEntryReader reader)
            {
                var doc = new DynamicJsonValue();
                foreach (var binding in _fieldMappings)
                {
                    if (binding.FieldIndexingMode is FieldIndexingMode.No || binding.FieldNameAsString is Client.Constants.Documents.Indexing.Fields.AllStoredFields)
                        continue;
                    var type = reader.GetFieldType(binding.FieldId, out var intOffset);
                    
                    
                    if ((type & IndexEntryFieldType.List) != 0)
                    {
                        var enumerableEntries = new List<object>();
                        if ((type & IndexEntryFieldType.SpatialPoint) != 0)
                        {
                            reader.GetReaderFor(binding.FieldId).TryReadManySpatialPoint(out var spatialIterator);
                            while (spatialIterator.ReadNext())
                            {
                                for (int i = 1; i <= spatialIterator.Geohash.Length; ++i)
                                    enumerableEntries.Add(Encodings.Utf8.GetString(spatialIterator.Geohash.Slice(0, i)));
                            }
                            doc[binding.FieldNameAsString] = enumerableEntries.ToArray();
                            continue;
                        }
                        
                        
                        reader.GetReaderFor(binding.FieldId).TryReadMany(out var iterator);
                        while (iterator.ReadNext())
                        {
                            if (iterator.IsNull || iterator.IsEmpty)
                                continue;
                            
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
                        reader.GetReaderFor(binding.FieldId).Read(out Span<byte> value);
                        if ((type & IndexEntryFieldType.SpatialPoint) != 0)
                        {
                            var enumerableEntries = new List<object>();
                            for (int geohashId = 1; geohashId <= value.Length; ++i)
                                enumerableEntries.Add(Encodings.Utf8.GetString(value.Slice(0, geohashId)));
                            doc[binding.FieldNameAsString] = enumerableEntries.ToArray();
                            continue;
                        }
                        
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

                if (maxTermLengthProceedPerAnalyzer[binding.FieldId] < value.Length)
                {
                    binding.Analyzer.GetOutputBuffersSize(value.Length, out var outputSize, out var tokenSize);
                    if (outputSize > encodedBuffer.Length)
                    {
                        Analyzer.BufferPool.Return(encodedBuffer);
                        encodedBuffer = Analyzer.BufferPool.Rent(_fieldMappings.MaximumOutputSize);
                        encoded = encodedBuffer.AsSpan();

                    }

                    if (tokenSize > tokensBuffer.Length)
                    {
                        Analyzer.TokensPool.Return(tokensBuffer);
                        tokensBuffer = Analyzer.TokensPool.Rent(_fieldMappings.MaximumTokenSize);
                        tokens = tokensBuffer.AsSpan();
                    }

                    maxTermLengthProceedPerAnalyzer[binding.FieldId] = value.Length;
                }
                
                binding.Analyzer.Execute(value, ref encoded, ref tokens);
                for (var index = 0; index < tokens.Length; ++index)
                {
                    token.ThrowIfCancellationRequested();
                    itemList.Add(Encodings.Utf8.GetString(encoded.Slice(tokens[index].Offset, (int)tokens[index].Length)));
                }

                return itemList.Count switch
                {
                    1 => itemList.First(),
                    > 1 => itemList.ToArray(),
                    _ => string.Empty
                };
            }
            
            int Skip()
            {
                while (true)
                {
                    token.ThrowIfCancellationRequested();
                    read = queryMatch.Fill(ids);
                    totalResults.Value += read;

                    if (position > read)
                    {
                        position -= read;
                        continue;
                    }

                    if (position == read)
                    {
                        read = queryMatch.Fill(ids);
                        totalResults.Value += read;
                        return 0;
                    }

                    return position;
                }
            }
        }

        public override IEnumerable<string> DynamicEntriesFields(HashSet<string> staticFields)
        {
            var fieldsInIndex = _indexSearcher.GetFields();
            foreach (var field in fieldsInIndex)
            {
                if (staticFields.Contains(field))
                    continue;
                yield return field;
            }
        }

        public override void Dispose()
        {
            base.Dispose();

            var exceptionAggregator = new ExceptionAggregator($"Could not dispose {nameof(CoraxIndexReadOperation)} of {_index.Name}");
            exceptionAggregator.Execute(() => _indexSearcher?.Dispose());
            exceptionAggregator.ThrowIfNeeded();
        }
    }
}
