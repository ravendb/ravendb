using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Threading;
using Corax.Querying;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Utils;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Explanation;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Client.Exceptions.Corax;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Highlightings;
using Raven.Server.Documents.Queries.MoreLikeThis.Corax;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server;
using Voron;
using Voron.Impl;
using Constants = Raven.Client.Constants;
using CoraxConstants = Corax.Constants;
using IndexSearcher = Corax.Querying.IndexSearcher;
using CoraxSpatialResult = global::Corax.Utils.Spatial.SpatialResult;
namespace Raven.Server.Documents.Indexes.Persistence.Corax
{
    public class CoraxIndexReadOperation : IndexReadOperationBase
    {
        // PERF: This is a hack in order to deal with RavenDB-19597. The ArrayPool creates contention under high requests environments.
        // There are 2 ways to avoid this contention, one is to avoid using it altogether and the other one is separating the pools from
        // the actual executing thread. While the correct approach would be to amp-up the usage of shared buffers (which would make) this
        // hack irrelevant, the complexity it introduces is much greater than what it make sense to be done at the moment. Therefore, 
        // we are building a quick fix that allow us to avoid the locking convoys and we will defer the real fix to RavenDB-19665. 
        [ThreadStatic] 
        private static ArrayPool<long> _queryPool;
        
        [ThreadStatic] 
        private static ArrayPool<float> _queryScorePool;

        [ThreadStatic] 
        private static ArrayPool<CoraxSpatialResult> _queryDistancePool;


        public static ArrayPool<long> QueryPool
        {
            get
            {
                _queryPool ??= ArrayPool<long>.Create();
                return _queryPool;
            }
        }

        private static ArrayPool<float> ScorePool
        {
            get
            {
                _queryScorePool ??= ArrayPool<float>.Create();
                return _queryScorePool;
            }
        }

        private static ArrayPool<CoraxSpatialResult> DistancePool 
        {
            get
            {
                _queryDistancePool ??= ArrayPool<CoraxSpatialResult>.Create();
                return _queryDistancePool;
            }
        }

        protected readonly IndexSearcher IndexSearcher;

        private readonly IndexFieldsMapping _fieldMappings;
        private readonly ByteStringContext _allocator;

        private readonly int _maxNumberOfOutputsPerDocument;

        private TermsReader _documentIdReader;


        public CoraxIndexReadOperation(Index index, Logger logger, Transaction readTransaction, QueryBuilderFactories queryBuilderFactories, IndexFieldsMapping fieldsMapping, IndexQueryServerSide query) : base(index, logger, queryBuilderFactories, query)
        {
            _allocator = readTransaction.Allocator;
            _fieldMappings = fieldsMapping;
            IndexSearcher = new IndexSearcher(readTransaction, _fieldMappings)
            {
                MaxMemoizationSizeInBytes = index.Configuration.MaxMemoizationSize.GetValue(SizeUnit.Bytes) 
            };
            if (index.Type.IsMap())
            {
                _documentIdReader = IndexSearcher.TermsReaderFor(Constants.Documents.Indexing.Fields.DocumentIdFieldName);
            }
            _maxNumberOfOutputsPerDocument = index.MaxNumberOfOutputsPerDocument;
        }

        public override long EntriesCount() => IndexSearcher.NumberOfEntries;
        
        protected interface ISupportsHighlighting
        {
            QueryTimingsScope TimingsScope { get; }
            Dictionary<string, CoraxHighlightingTermIndex> Terms { get; }
            void Initialize(IndexQueryServerSide query, QueryTimingsScope scope);
            void Setup(IndexQueryServerSide query, DocumentsOperationContext context);

            Dictionary<string, Dictionary<string, string[]>> Execute(IndexQueryServerSide query, DocumentsOperationContext context, IndexFieldsMapping fieldMappings,
                ref EntryTermsReader entryReader, FieldsToFetch highlightingFields, Document document, IndexSearcher indexSearcher);
        }

        private struct NoHighlighting : ISupportsHighlighting
        {
            public QueryTimingsScope TimingsScope => null;
            public Dictionary<string, CoraxHighlightingTermIndex> Terms => null;
            public void Initialize(IndexQueryServerSide query, QueryTimingsScope scope) { }
            public void Setup(IndexQueryServerSide query, DocumentsOperationContext context) { }

            public Dictionary<string, Dictionary<string, string[]>> Execute(IndexQueryServerSide query, DocumentsOperationContext context,
                IndexFieldsMapping fieldMappings, ref EntryTermsReader entryReader, FieldsToFetch highlightingFields, Document document, IndexSearcher indexSearcher)
                => null;
        }

        private struct HasHighlighting : ISupportsHighlighting
        {
            private QueryTimingsScope _timingsScope;
            private Dictionary<string, CoraxHighlightingTermIndex> _terms;

            public QueryTimingsScope TimingsScope => _timingsScope;
            public Dictionary<string, CoraxHighlightingTermIndex> Terms => _terms;

            public void Initialize(IndexQueryServerSide query, QueryTimingsScope scope)
            {
                _timingsScope = scope?.For(nameof(QueryTimingsScope.Names.Highlightings), start: false);
                _terms = new Dictionary<string, CoraxHighlightingTermIndex>();
            }

            public void Setup(IndexQueryServerSide query, DocumentsOperationContext context)
            {
                using (_timingsScope?.For(nameof(QueryTimingsScope.Names.Setup)))
                {
                    foreach (var term in Terms)
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
                                nls = new string[as1.Length];
                                for (int i = 0; i < as1.Length; i++)
                                    nls[i] = as1[i].TrimEnd('*').TrimStart('*');
                                break;
                            case null:
                                continue;
                            default:
                                throw new NotSupportedException($"The type '{term.Value.Values.GetType().FullName}' is not supported.");
                        }

                        term.Value.Values = nls;
                        term.Value.PreTags = null;
                        term.Value.PostTags = null;
                    }

                    var highlightingTerms = _terms;
                    foreach (var highlighting in query.Metadata.Highlightings)
                    {
                        var options = highlighting.GetOptions(context, query.QueryParameters);
                        if (options == null)
                            continue;

                        var numberOfPreTags = options.PreTags?.Length ?? 0;
                        var numberOfPostTags = options.PostTags?.Length ?? 0;
                        if (numberOfPreTags != numberOfPostTags)
                            throw new InvalidOperationException("Number of pre-tags and post-tags must match.");

                        var fieldName = highlighting.Field.Value;

                        if (highlightingTerms.TryGetValue(fieldName, out var termIndex) == false)
                        {
                            // the case when we have to create MapReduce highlighter
                            termIndex = new()
                            {
                                FieldName = highlighting.Field.Value,
                                DynamicFieldName = AutoIndexField.GetSearchAutoIndexFieldName(highlighting.Field.Value),
                                GroupKey = options.GroupKey
                            };
                            highlightingTerms.Add(query.Metadata.IsDynamic ? termIndex.DynamicFieldName : termIndex.FieldName, termIndex);
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
            }

            public Dictionary<string, Dictionary<string, string[]>> Execute(IndexQueryServerSide query, DocumentsOperationContext context,
                IndexFieldsMapping fieldMappings, ref EntryTermsReader entryReader, FieldsToFetch highlightingFields, Document document, IndexSearcher indexSearcher)
            {
                using (_timingsScope?.For(nameof(QueryTimingsScope.Names.Fill)))
                {
                    var highlightings = new Dictionary<string, Dictionary<string, string[]>>();
                    var highlightingTerms = Terms;
                    var allocator = context.Allocator;

                    // If we have highlightings then we need to setup the Corax objects that will attach to the evaluator in order
                    // to retrieve the fields and perform the transformations required by Highlightings. 
                    foreach (var current in query.Metadata.Highlightings)
                    {
                        // We get the actual highlight description. 
                        var fieldName = current.Field.Value;
                        string key = document.Id;

                        if (highlightingTerms.TryGetValue(fieldName, out var fieldDescription) == false)
                        {
                            continue;
                        }

                        
                        //We have to get analyzer so dynamic field have priority over normal name
                        // We get the field binding to ensure that we are running the analyzer to find the actual tokens.
                        if (fieldMappings.TryGetByFieldName(allocator, fieldDescription.DynamicFieldName ?? fieldDescription.FieldName, out var fieldBinding) == false)
                            continue;

                        // We will get the actual tokens dictionary for this field. If it exists we get it immediately, if not we create
                        if (highlightings.TryGetValue(fieldDescription.FieldName, out var tokensDictionary) == false)
                        {
                            tokensDictionary = new(StringComparer.OrdinalIgnoreCase);
                            highlightings[fieldDescription.FieldName] = tokensDictionary;
                        }

                        List<string> fragments = new();
                        
                        // We need to get the actual field, not the dynamic field. 
                        int propIdx = document.Data.GetPropertyIndex(fieldDescription.FieldName);
                        if (propIdx < 0)
                        {
                            bool isDirectlyFromIndex = false;
                            
                            long fieldRootPage = indexSearcher.FieldCache.GetLookupRootPage(fieldName);
                            entryReader.Reset();
                            int maxFragments = current.FragmentCount;
                            while (entryReader.MoveNextStoredField())
                            {
                                if (entryReader.FieldRootPage != fieldRootPage)
                                    continue;

                                isDirectlyFromIndex = true;
                                
                                if (entryReader.StoredField == null)
                                    break;

                                if (entryReader.IsRaw)
                                    break;
                                
                                var span = entryReader.StoredField.Value;
                                var fieldValue = span.ToStringValue();
                                
                                if (entryReader.IsList)
                                {
                                    maxFragments -= ProcessHighlightings(current, fieldDescription, fieldValue, fragments, maxFragments);
                                    continue;
                                }
                                
                                ProcessHighlightings(current, fieldDescription, fieldValue, fragments, current.FragmentCount);
                            }

                            if (isDirectlyFromIndex == false)
                                continue;
                            else
                                goto Finish;
                        }

                        BlittableJsonReaderObject.PropertyDetails property = default;
                        document.Data.GetPropertyByIndex(propIdx, ref property);

                        if (property.Token == BlittableJsonToken.String)
                        {
                            var fieldValue = ((LazyStringValue)property.Value).ToString(CultureInfo.InvariantCulture);
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
                        else 
                            continue;

                        if (string.IsNullOrWhiteSpace(fieldDescription.GroupKey) == false)
                        {
                            int groupKey;
                            if ((groupKey = document.Data.GetPropertyIndex(fieldDescription.GroupKey)) != -1)
                            {
                                document.Data.GetPropertyByIndex(groupKey, ref property);

                                key = property.Token switch
                                {
                                    BlittableJsonToken.String => ((LazyStringValue)property.Value).ToString(CultureInfo.InvariantCulture),
                                    BlittableJsonToken.CompressedString => ((LazyCompressedStringValue)property.Value).ToString(),
                                    _ => throw new NotSupportedException($"The token type '{property.Token.ToString()}' is not supported.")
                                };
                            }
                        }
                        
                        Finish:
                        if (fragments.Count <= 0) 
                            continue;
                            
                        if (tokensDictionary.TryGetValue(key, out var existingHighlights))
                            throw new NotSupportedInCoraxException("Multiple highlightings for the same field and group key are not supported.");

                        tokensDictionary[key] = fragments.ToArray();
                    }

                    return highlightings;
                }
            }
        }

        protected interface IHasDistinct
        {
        }

        private struct NoDistinct : IHasDistinct { }

        private struct HasDistinct : IHasDistinct { }


        // Even if there are no distinct statements we have to be sure that we are not including
        // documents that we have already included during this request. 
        protected struct IdentityTracker<TDistinct> where TDistinct : struct, IHasDistinct
        {
            private Index _index;
            private IndexQueryServerSide _query;
            private IndexSearcher _searcher;
            private IndexFieldsMapping _fieldsMapping;
            private IQueryResultRetriever<QueriedDocument> _retriever;

            private bool _isMap;

            private GrowableHashSet<UnmanagedSpan> _alreadySeenDocumentKeysInPreviousPage;
            private GrowableHashSet<ulong> _alreadySeenProjections;
            public long QueryStart;
            private TermsReader _documentIdReader;

            public void Initialize(Index index, IndexQueryServerSide query, IndexSearcher searcher, TermsReader documentIdReader, IndexFieldsMapping fieldsMapping, IQueryResultRetriever<QueriedDocument> retriever)
            {
                _index = index;
                _query = query;
                _searcher = searcher;
               
                _fieldsMapping = fieldsMapping;
                _retriever = retriever;
                _documentIdReader = documentIdReader; 
                _alreadySeenDocumentKeysInPreviousPage = new(UnmanagedSpanComparer.Instance);
                QueryStart = _query.Start;
                _isMap = index.Type.IsMap();
               
            }

            public long RegisterDuplicates<TProjection>(ref TProjection hasProjection, long currentIdx, ReadOnlySpan<long> ids, CancellationToken token)
                where TProjection : struct, IHasProjection
            {
                // From now on, we know we will try to skip duplicates.
                long limit;

                // If query start is effectively bigger than the one we are starting on. 
                if (QueryStart > currentIdx)
                {
                    // If the query start before the current read ids, then we have to divide the ids in those
                    // that need to be processed for discarding and those that don't. 
                    if (QueryStart < currentIdx + ids.Length)
                        limit = QueryStart - currentIdx;
                    else
                        limit = ids.Length;
                }
                else return 0; // we left it behind, so we are going to continue going for 0. 

                var distinctIds = ids.Slice(0, (int)limit);

                if (_isMap && hasProjection.IsProjection == false)
                {
                    // Assumptions: we're in Map, so that mean we have ID of the doc saved in the tree. So we want to keep track what we returns
                    foreach (var id in distinctIds)
                    {
                        _documentIdReader.TryGetRawTermFor(id, out var key);
                        _alreadySeenDocumentKeysInPreviousPage.Add(key);
                    }

                    return limit;
                }

                if (typeof(TDistinct) == typeof(HasDistinct))
                {
                    _alreadySeenProjections ??= new();

                    var retriever = _retriever;
                    Page page = default;
                    foreach (var id in distinctIds)
                    {
                        var reader = _searcher.GetEntryTermsReader(id, ref page);

                        var key = _documentIdReader.GetTermFor(id);
                        var retrieverInput = new RetrieverInput(_searcher, _fieldsMapping, reader, key, _index.IndexFieldsPersistence.HasTimeValues);
                        var result = retriever.Get(ref retrieverInput, token);

                        if (result.Document != null)
                        {
                            if (result.Document.Data.Count > 0)
                            {
                                // we don't consider empty projections to be relevant for distinct operations
                                _alreadySeenProjections.Add(result.Document.DataHash);
                            }
                        }
                        else if (result.List != null)
                        {
                            foreach (Document item in result.List)
                            {
                                if (item.Data.Count > 0)
                                {
                                    // we don't consider empty projections to be relevant for distinct operations
                                    _alreadySeenProjections.Add(item.DataHash);
                                }
                            }
                        }
                    }
                }

                return limit;
            }

            public bool ShouldIncludeIdentity<TProjection>(ref TProjection hasProjection, UnmanagedSpan identity)
                where TProjection : struct, IHasProjection
            {
                return hasProjection.IsProjection || _alreadySeenDocumentKeysInPreviousPage.Add(identity);
            }

            public bool ShouldIncludeDocument<TProjection>(ref TProjection hasProjection, Document doc)
                where TProjection : struct, IHasProjection
            {
                if (doc == null)
                    return false;

                if (typeof(TDistinct) == typeof(HasDistinct))
                {
                    _alreadySeenProjections ??= new();
                    if (_alreadySeenProjections.Add(doc.DataHash) == false)
                        return false;
                }

                return true;
            }
        }

        protected interface ISupportsQueryFilter : IDisposable
        {
            FilterResult Apply(ref RetrieverInput input, string key);
        }

        private readonly struct NoQueryFilter : ISupportsQueryFilter
        {
            public void Dispose() { }

            public FilterResult Apply(ref RetrieverInput input, string key) => FilterResult.Accepted;
        }

        private readonly struct HasQueryFilter : ISupportsQueryFilter
        {
            private readonly QueryFilter _filter;
            public HasQueryFilter([NotNull] QueryFilter filter)
            {
                _filter = filter;
            }
            public void Dispose()
            {
                _filter.Dispose();
            }

            public FilterResult Apply(ref RetrieverInput input, string key) => _filter.Apply(ref input, key);
        }

        protected interface IHasProjection
        {
            bool IsProjection { get; }
        }

        private struct NoProjection : IHasProjection
        {
            public bool IsProjection => false;
        }

        private struct HasProjection : IHasProjection
        {
            public bool IsProjection => true;
        }

        private static bool WillAlwaysIncludeInResults(IndexType indexType, FieldsToFetch fieldsToFetch, IndexQueryServerSide query)
        {
            return fieldsToFetch.IsDistinct || query.SkipDuplicateChecking || indexType.IsMapReduce();
        }

        private IEnumerable<QueryResult> QueryInternal<THighlighting, TQueryFilter, THasProjection, TDistinct>(
                    IndexQueryServerSide query, QueryTimingsScope queryTimings, FieldsToFetch fieldsToFetch,
                    Reference<long> totalResults, Reference<long> skippedResults, Reference<long> scannedDocuments,
                    IQueryResultRetriever<QueriedDocument> retriever, DocumentsOperationContext documentsContext,
                    Func<string, SpatialField> getSpatialField,
                    CancellationToken token)
                where TDistinct : struct, IHasDistinct
                where THasProjection : struct, IHasProjection
                where THighlighting : struct, ISupportsHighlighting
                where TQueryFilter : struct, ISupportsQueryFilter
        {
            // The query method will have to deal with 2 different usages. The first is when the user requests a query and everything
            // fits into a single page. In those cases it is easy because the client pages size and the internal Corax buffer size
            // may be the same and will not introduce any inconsistency. 

            // However, the user may not care about the page size because he is streaming or counting. In those cases the process itself
            // will finish when the user either stops or we don't have any more matches to deal with. 

            // In the case of distinct, pagination is not such a big deal because we will have to calculate distinct anyways therefore
            // we can just count the current returned document number, number of skipped documents but in the end we just iterating over
            // the entire set. As we don't keep track of 'follow up' information when the next page comes, we will just recalculate the
            // distinct.


            var identityTracker = new IdentityTracker<TDistinct>();
            identityTracker.Initialize(_index, query, IndexSearcher, _documentIdReader, _fieldMappings, retriever);

            long pageSize = query.PageSize;

            if (query.Metadata.HasExplanations)
                ThrowExplanationsIsNotImplementedInCorax();

            long take = pageSize + query.Start;
            if (take > IndexSearcher.NumberOfEntries || fieldsToFetch.IsDistinct)
                take = CoraxConstants.IndexSearcher.TakeAll;

            bool isDistinctCount = query.PageSize == 0 && typeof(TDistinct) == typeof(HasDistinct);
            if (isDistinctCount)
            {
                if (pageSize > int.MaxValue)
                    ThrowDistinctOnBiggerCollectionThanInt32();

                pageSize = int.MaxValue;
                take = CoraxConstants.IndexSearcher.TakeAll;
            }

            THasProjection hasProjections = default;
            THighlighting highlightings = default;
            highlightings.Initialize(query, queryTimings);

            long docsToLoad = pageSize;
            bool runQuery = true;
            while (runQuery)
            {
                IQueryMatch queryMatch;
                OrderMetadata[] orderByFields;

                CoraxQueryBuilder.Parameters builderParameters;
                using (queryTimings?.For(nameof(QueryTimingsScope.Names.Corax), start: false)?.Start())
                {
                    IDisposable releaseServerContext = null;
                    IDisposable closeServerTransaction = null;
                    TransactionOperationContext serverContext = null;

                    try
                    {
                        if (query.Metadata.HasCmpXchg)
                        {
                            releaseServerContext = documentsContext.DocumentDatabase.ServerStore.ContextPool.AllocateOperationContext(out serverContext);
                            closeServerTransaction = serverContext.OpenReadTransaction();
                        }

                        builderParameters = new CoraxQueryBuilder.Parameters(IndexSearcher, _allocator, serverContext, documentsContext, query, _index,
                            query.QueryParameters, QueryBuilderFactories, _fieldMappings, fieldsToFetch, highlightings.Terms, (int)take, indexReadOperation: this, token: token);

                        using (closeServerTransaction)
                        {
                            if ((queryMatch = CoraxQueryBuilder.BuildQuery(builderParameters, out orderByFields)) is null)
                                yield break;
                        }

                        queryTimings?.SetQueryPlan(queryMatch.Inspect());
                    }
                    finally
                    {
                        releaseServerContext?.Dispose();
                    }
                }

                highlightings.Setup(query, documentsContext);

                int bufferSize = CoraxBufferSize(IndexSearcher, take, query);
                var ids = QueryPool.Rent(bufferSize);
                SortingDataTransfer sortingData = default;
                using var queryFilter = GetQueryFilter();
                Page page = default;
                bool willAlwaysIncludeInResults = WillAlwaysIncludeInResults(_index.Type, fieldsToFetch, query);
                totalResults.Value = 0;

                var hasOrderByDistance = query.Metadata.OrderBy is [{OrderingType: OrderByFieldType.Distance}, ..] && _index.Configuration.CoraxIncludeSpatialDistance;
                if (builderParameters.HasBoost || hasOrderByDistance)
                {
                    sortingData = new()
                    {
                        ScoresBuffer = _index.Configuration.CoraxIncludeDocumentScore && builderParameters is {HasBoost: true}
                            ? ScorePool.Rent(bufferSize)
                            : null,
                        DistancesBuffer = _index.Configuration.CoraxIncludeSpatialDistance && hasOrderByDistance
                            ? DistancePool.Rent(bufferSize)
                            : null
                    };
                    
                    switch (queryMatch)
                    {
                        case SortingMatch sm:
                            sm.SetScoreAndDistanceBuffer(sortingData);
                            queryMatch = sm;
                            break;
                        case SortingMultiMatch smm:
                            smm.SetSortingDataTransfer(sortingData);
                            queryMatch = smm;
                            break;
                    }
                }

                // We don't need to do any processing for the query beyond counting if we are getting a count.
                while (query.IsCountQuery == false || typeof(TDistinct) == typeof(HasDistinct))
                {
                    token.ThrowIfCancellationRequested();

                    // We look for items that was haven't seen before in the case of paging. 
                    int read = queryMatch.Fill(ids);
                    if (read == 0)
                        goto Done;

                    // If we are going to skip, we've better do it knowing how many we have passed. 
                    long i = identityTracker.RegisterDuplicates(ref hasProjections, totalResults.Value, ids.AsSpan(0, read), token);
                    totalResults.Value += read; // important that this is *after* RegisterDuplicates

                    // Now for every document that was selected. document it. 
                    for (; docsToLoad != 0 && i < read; ++i, --docsToLoad)
                    {
                        token.ThrowIfCancellationRequested();

                        long indexEntryId = ids[i];

                        // If we are going to include no matter what, lets skip everything else.
                        if (willAlwaysIncludeInResults)
                            goto Include;

                        // Ok, we will need to check for duplicates, then we will have to work. In some cases (like TimeSeries) we don't "have" unique identifier so we skip checking.
                        var identityExists = retriever.TryGetKeyCorax(_documentIdReader, indexEntryId, out var rawIdentity);

                        // If we have figured out that this document identity has already been seen, we are skipping it.
                        if (identityExists && identityTracker.ShouldIncludeIdentity(ref hasProjections, rawIdentity) == false)
                        {
                            docsToLoad++;
                            skippedResults.Value++;
                            continue;
                        }

                        if (typeof(TDistinct) == typeof(HasDistinct) && query.IsCountQuery)
                            continue;

                        // Now we know this is a new candidate document to be return therefore, we are going to be getting the
                        // actual data and apply the rest of the filters. 
                        Include:

                        float? documentScore = sortingData.IncludeScores ? sortingData.ScoresBuffer[i] : null;
                        CoraxSpatialResult? documentDistance = hasOrderByDistance ? sortingData.DistancesBuffer[i] : null;

                        var key = _documentIdReader.GetTermFor(indexEntryId);
                        EntryTermsReader entryTermsReader = IndexSearcher.GetEntryTermsReader(indexEntryId, ref page);
                        var retrieverInput = new RetrieverInput(IndexSearcher, _fieldMappings, in entryTermsReader, key, _index.IndexFieldsPersistence.HasTimeValues, documentScore, documentDistance);

                        var filterResult = queryFilter.Apply(ref retrieverInput, key);
                        if (filterResult is not FilterResult.Accepted)
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
                            var qr = CreateQueryResult(ref identityTracker, fetchedDocument.Document, query, documentsContext, ref entryTermsReader, fieldsToFetch, orderByFields, ref highlightings, skippedResults, ref hasProjections, ref markedAsSkipped);
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
                                var qr = CreateQueryResult(ref identityTracker, item, query, documentsContext, ref entryTermsReader, fieldsToFetch, orderByFields, ref highlightings, skippedResults, ref hasProjections, ref markedAsSkipped);
                                if (qr.Result is null)
                                {
                                    docsToLoad++;
                                    continue;
                                }

                                yield return qr;
                            }
                        }
                        else
                        {
                            skippedResults.Value++;
                        }
                    }

                    // No need to continue filling buffers as there are no more docs to load and we are skipping statistics anyways.
                    if (docsToLoad <= 0)
                        break;
                }


                // If we are going to just return count() then we don't care about anything else than memoize the results.
                if (query.IsCountQuery || query.SkipStatistics == false)
                {
                    int read;
                    do
                    {
                        // Instead of memoizing, we just continue filling the buffer. First, because we don't need to keep the 
                        // value or deduplicate at this stage; just to know how many potential matches we have left. Also memoizing
                        // is not supported for SortingMatch. 
                        read = queryMatch.Fill(ids);
                        totalResults.Value += read;
                    } 
                    while (read != 0);
                }

                Done:

                QueryPool.Return(ids);
                if (sortingData.IncludeScores)
                    ScorePool.Return(sortingData.ScoresBuffer);
                if (sortingData.IncludeDistances)
                    DistancePool.Return(sortingData.DistancesBuffer);
                
                if (queryMatch is not SortingMatch && queryMatch is not SortingMultiMatch)
                    break; // this is only relevant if we are sorting, since we may have filtered items and need to read more, see: RavenDB-20294

                var sortingMatchTotalResults = 0L;
                if (queryMatch is SortingMatch) 
                    sortingMatchTotalResults = ((SortingMatch)queryMatch).TotalResults;
                else 
                    sortingMatchTotalResults = ((SortingMultiMatch)queryMatch).TotalResults;

                if (docsToLoad == 0 ||
                    sortingMatchTotalResults == totalResults.Value ||
                    scannedDocuments.Value >= query.FilterLimit)
                {
                    totalResults.Value = (int)Math.Min(sortingMatchTotalResults, int.MaxValue);
                    runQuery = false;
                }
                else
                {
                    Debug.Assert(_maxNumberOfOutputsPerDocument > 0);
                    take += (pageSize - (pageSize - docsToLoad)) * _maxNumberOfOutputsPerDocument;
                    if (take < 0) // handle overflow
                        take = int.MaxValue;
                    // start *after* all the items we already read and returned to the caller
                    identityTracker.QueryStart = totalResults.Value;
                }
            }

            if (isDistinctCount)
                totalResults.Value -= skippedResults.Value;

            TQueryFilter GetQueryFilter()
            {
                if (typeof(TQueryFilter) == typeof(NoQueryFilter))
                    return (TQueryFilter)(object)new NoQueryFilter();
                if (typeof(TQueryFilter) == typeof(HasQueryFilter))
                {
                    return (TQueryFilter)(object)new HasQueryFilter(
                        new QueryFilter(_index, query, documentsContext, skippedResults, scannedDocuments, retriever, queryTimings)
                    );
                }
                
                throw new NotSupportedException($"The type {typeof(TQueryFilter)} is not supported.");
            }
        }

        protected virtual QueryResult CreateQueryResult<TDistinct, THasProjection, THighlighting>(ref IdentityTracker<TDistinct> tracker, Document document,
            IndexQueryServerSide query, DocumentsOperationContext documentsContext, ref EntryTermsReader entryReader, FieldsToFetch highlightingFields, OrderMetadata[] orderByFields, ref THighlighting highlightings,
            Reference<long> skippedResults,
            ref THasProjection hasProjections, ref bool markedAsSkipped)
            where TDistinct : struct, IHasDistinct
            where THasProjection : struct, IHasProjection
            where THighlighting : struct, ISupportsHighlighting
        {
            if (tracker.ShouldIncludeDocument(ref hasProjections, document) == false)
            {
                document?.Dispose();

                if (markedAsSkipped == false)
                {
                    skippedResults.Value++;
                    markedAsSkipped = true;
                }

                return default;
            }

            return new QueryResult
            {
                Result = document,
                Highlightings = highlightings.Execute(query, documentsContext, _fieldMappings, ref entryReader, highlightingFields, document, IndexSearcher),
            };
        }

        public override IEnumerable<QueryResult> Query(IndexQueryServerSide query, QueryTimingsScope queryTimings, FieldsToFetch fieldsToFetch,
            Reference<long> totalResults, Reference<long> skippedResults,
            Reference<long> scannedDocuments, IQueryResultRetriever<QueriedDocument> retriever, DocumentsOperationContext documentsContext, Func<string, SpatialField> getSpatialField,
            CancellationToken token)
        {
            if (query.Metadata.HasHighlightings)
            {
                if (query.Metadata.FilterScript is null)
                {
                    if (fieldsToFetch.IsProjection)
                    {
                        if (query.Metadata.IsDistinct)
                        {
                            return QueryInternal<HasHighlighting, NoQueryFilter, HasProjection, HasDistinct>(
                                query, queryTimings, fieldsToFetch,
                                totalResults, skippedResults, scannedDocuments,
                                retriever, documentsContext,
                                getSpatialField,
                                token);
                        }
                        else
                        {
                            return QueryInternal<HasHighlighting, NoQueryFilter, HasProjection, NoDistinct>(
                                query, queryTimings, fieldsToFetch,
                                totalResults, skippedResults, scannedDocuments,
                                retriever, documentsContext,
                                getSpatialField,
                                token);
                        }
                    }
                    else
                    {
                        if (query.Metadata.IsDistinct)
                        {
                            return QueryInternal<HasHighlighting, NoQueryFilter, NoProjection, HasDistinct>(
                                query, queryTimings, fieldsToFetch,
                                totalResults, skippedResults, scannedDocuments,
                                retriever, documentsContext,
                                getSpatialField,
                                token);
                        }
                        else
                        {
                            return QueryInternal<HasHighlighting, NoQueryFilter, NoProjection, NoDistinct>(
                                query, queryTimings, fieldsToFetch,
                                totalResults, skippedResults, scannedDocuments,
                                retriever, documentsContext,
                                getSpatialField,
                                token);
                        }
                    }
                }
                else
                {
                    if (fieldsToFetch.IsProjection)
                    {
                        if (query.Metadata.IsDistinct)
                        {
                            return QueryInternal<HasHighlighting, HasQueryFilter, HasProjection, HasDistinct>(
                                query, queryTimings, fieldsToFetch,
                                totalResults, skippedResults, scannedDocuments,
                                retriever, documentsContext,
                                getSpatialField,
                                token);
                        }
                        else
                        {
                            return QueryInternal<HasHighlighting, HasQueryFilter, HasProjection, NoDistinct>(
                                query, queryTimings, fieldsToFetch,
                                totalResults, skippedResults, scannedDocuments,
                                retriever, documentsContext,
                                getSpatialField,
                                token);
                        }
                    }
                    else
                    {
                        if (query.Metadata.IsDistinct)
                        {
                            return QueryInternal<HasHighlighting, HasQueryFilter, NoProjection, HasDistinct>(
                                query, queryTimings, fieldsToFetch,
                                totalResults, skippedResults, scannedDocuments,
                                retriever, documentsContext,
                                getSpatialField,
                                token);
                        }
                        else
                        {
                            return QueryInternal<HasHighlighting, HasQueryFilter, NoProjection, NoDistinct>(
                                query, queryTimings, fieldsToFetch,
                                totalResults, skippedResults, scannedDocuments,
                                retriever, documentsContext,
                                getSpatialField,
                                token);
                        }
                    }
                }
            }
            else
            {
                if (query.Metadata.FilterScript is null)
                {
                    if (fieldsToFetch.IsProjection)
                    {
                        if (query.Metadata.IsDistinct)
                        {
                            return QueryInternal<NoHighlighting, NoQueryFilter, HasProjection, HasDistinct>(
                                query, queryTimings, fieldsToFetch,
                                totalResults, skippedResults, scannedDocuments,
                                retriever, documentsContext,
                                getSpatialField,
                                token);
                        }
                        else
                        {
                            return QueryInternal<NoHighlighting, NoQueryFilter, HasProjection, NoDistinct>(
                                query, queryTimings, fieldsToFetch,
                                totalResults, skippedResults, scannedDocuments,
                                retriever, documentsContext,
                                getSpatialField,
                                token);
                        }
                    }
                    else
                    {
                        if (query.Metadata.IsDistinct)
                        {
                            return QueryInternal<NoHighlighting, NoQueryFilter, NoProjection, HasDistinct>(
                                query, queryTimings, fieldsToFetch,
                                totalResults, skippedResults, scannedDocuments,
                                retriever, documentsContext,
                                getSpatialField,
                                token);
                        }
                        else
                        {
                            return QueryInternal<NoHighlighting, NoQueryFilter, NoProjection, NoDistinct>(
                                query, queryTimings, fieldsToFetch,
                                totalResults, skippedResults, scannedDocuments,
                                retriever, documentsContext,
                                getSpatialField,
                                token);
                        }
                    }
                }
                else
                {
                    if (fieldsToFetch.IsProjection)
                    {
                        if (query.Metadata.IsDistinct)
                        {
                            return QueryInternal<NoHighlighting, HasQueryFilter, HasProjection, HasDistinct>(
                                query, queryTimings, fieldsToFetch,
                                totalResults, skippedResults, scannedDocuments,
                                retriever, documentsContext,
                                getSpatialField,
                                token);
                        }
                        else
                        {
                            return QueryInternal<NoHighlighting, HasQueryFilter, HasProjection, NoDistinct>(
                                query, queryTimings, fieldsToFetch,
                                totalResults, skippedResults, scannedDocuments,
                                retriever, documentsContext,
                                getSpatialField,
                                token);
                        }
                    }
                    else
                    {
                        if (query.Metadata.IsDistinct)
                        {
                            return QueryInternal<NoHighlighting, HasQueryFilter, NoProjection, HasDistinct>(
                                query, queryTimings, fieldsToFetch,
                                totalResults, skippedResults, scannedDocuments,
                                retriever, documentsContext,
                                getSpatialField,
                                token);
                        }
                        else
                        {
                            return QueryInternal<NoHighlighting, HasQueryFilter, NoProjection, NoDistinct>(
                                query, queryTimings, fieldsToFetch,
                                totalResults, skippedResults, scannedDocuments,
                                retriever, documentsContext,
                                getSpatialField,
                                token);
                        }
                    }
                }
            }
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

                    int expectedFragmentRestEnd = Math.Min(current.FragmentLength - tokenEnd, fieldFragment.Length);
                    string fragment;
                    if (expectedFragmentRestEnd < 0)
                    {
                        fragment = $"{preTag}{fieldFragment[tokenStart..tokenEnd]}{postTag}";
                    }
                    else
                    {
                        var fieldFragmentSpan = fieldFragment.Length - tokenEnd < expectedFragmentRestEnd
                                                    ? fieldFragment.Slice(tokenEnd)
                                                    : fieldFragment.Slice(tokenEnd, expectedFragmentRestEnd);

                        int fragmentEnd = fieldFragmentSpan.LastIndexOf(' ');
                        if (fragmentEnd > 0)
                            expectedFragmentRestEnd = tokenEnd + fragmentEnd;
                        else
                            expectedFragmentRestEnd = fieldFragment.Length;

                        fragment = $"{preTag}{fieldFragment[tokenStart..tokenEnd]}{postTag}{fieldFragment[tokenEnd..expectedFragmentRestEnd]}";
                    }

                    fragments.Add(fragment);

                    totalFragments++;
                    currentIndex = tokenEnd;
                }
            }

            return totalFragments;
        }

        public override IEnumerable<QueryResult> IntersectQuery(IndexQueryServerSide query, FieldsToFetch fieldsToFetch, Reference<long> totalResults,
            Reference<long> skippedResults, Reference<long> scannedDocuments, IQueryResultRetriever<QueriedDocument> retriever,
            DocumentsOperationContext documentsContext, Func<string, SpatialField> getSpatialField, CancellationToken token)
        {
            throw new NotImplementedException($"{nameof(Corax)} does not support intersect queries.");
        }

        public override SortedSet<string> Terms(string field, string fromValue, long pageSize, CancellationToken token)
        {
            SortedSet<string> results = new();

            if (IndexSearcher.TryGetTermsOfField(IndexSearcher.FieldMetadataBuilder(field), out var terms) == false)
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

        public override IEnumerable<QueryResult> MoreLikeThis(IndexQueryServerSide query, IQueryResultRetriever<QueriedDocument> retriever, DocumentsOperationContext context,
            CancellationToken token)
        {
            IDisposable releaseServerContext = null;
            IDisposable closeServerTransaction = null;
            TransactionOperationContext serverContext = null;
            MoreLikeThisQuery moreLikeThisQuery;
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
                    builderParameters = new(IndexSearcher, _allocator, serverContext, context, query, _index, query.QueryParameters, QueryBuilderFactories,
                        _fieldMappings, null, null /* allow highlighting? */, CoraxQueryBuilder.TakeAll, indexReadOperation: this, token: token);
                    moreLikeThisQuery = CoraxQueryBuilder.BuildMoreLikeThisQuery(builderParameters, query.Metadata.Query.Where);
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

            builderParameters = new(IndexSearcher, _allocator, null, context, query, _index, query.QueryParameters, QueryBuilderFactories,
                _fieldMappings, null, null /* allow highlighting? */, CoraxQueryBuilder.TakeAll, indexReadOperation: this, token: token);
            using var mlt = new RavenRavenMoreLikeThis(builderParameters, options);
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

            var pageSize = CoraxBufferSize(IndexSearcher, query.PageSize, query);

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
                mltQuery = IndexSearcher.And(mltQuery, moreLikeThisQuery.FilterQuery);
            }

            var ravenIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            long[] ids = QueryPool.Rent(pageSize);
            var read = 0;
            long returnedDocs = 0;
            Page page = default;
            while ((read = mltQuery.Fill(ids.AsSpan())) != 0)
            {
                for (int i = 0; i < read; i++)
                {
                    if (returnedDocs >= query.Limit)
                        yield break;
                    
                    var hit = ids[i];
                    token.ThrowIfCancellationRequested();

                    if (hit == baseDocId)
                        continue;

                    var termsReader = IndexSearcher.GetEntryTermsReader(hit, ref page);
                    var id = _documentIdReader.GetTermFor(hit);

                    if (ravenIds.Add(id) == false)
                        continue;

                    var retrieverInput = new RetrieverInput(IndexSearcher, _fieldMappings, termsReader, id, _index.IndexFieldsPersistence.HasTimeValues);
                    var result = retriever.Get(ref retrieverInput, token);
                    
                    if (result.Document != null)
                    {
                        returnedDocs++;
                        yield return new QueryResult { Result = result.Document };
                    }
                    else if (result.List != null)
                    {
                        foreach (Document item in result.List)
                        {
                            returnedDocs++;
                            yield return new QueryResult { Result = item };
                        }
                    }
                }
            }

            QueryPool.Return(ids);
        }

        public override IEnumerable<BlittableJsonReaderObject> IndexEntries(IndexQueryServerSide query, Reference<long> totalResults,
            DocumentsOperationContext documentsContext, Func<string, SpatialField> getSpatialField, bool ignoreLimit, CancellationToken token)
        {
            var pageSize = query.PageSize;
            var position = query.Start;

            if (query.Metadata.IsDistinct)
                throw new NotSupportedInCoraxException("We don't support Distinct in \"Show Raw Entry\" of Index.");
            if (query.Metadata.FilterScript != null)
                throw new NotSupportedInCoraxException(
                    "Filter isn't supported in Raw Index View.");

            var take = pageSize + position;
            if (take > IndexSearcher.NumberOfEntries)
                take = CoraxConstants.IndexSearcher.TakeAll;

            IQueryMatch queryMatch;
            var builderParameters = new CoraxQueryBuilder.Parameters(IndexSearcher, _allocator, null, null, query, _index, query.QueryParameters, QueryBuilderFactories, _fieldMappings, null, null, -1, indexReadOperation: this, token: token);
            if ((queryMatch = CoraxQueryBuilder.BuildQuery(builderParameters, out _)) is null)
                yield break;

            var ids = QueryPool.Rent(CoraxBufferSize(IndexSearcher, take, query));
            int docsToLoad = CoraxBufferSize(IndexSearcher, pageSize, query);
            using var coraxEntryReader = new CoraxIndexedEntriesReader(documentsContext, IndexSearcher);
            int read;
            long i = Skip();
            Page page = default;
            while (true)
            {
                token.ThrowIfCancellationRequested();
                for (; docsToLoad != 0 && i < read; ++i, --docsToLoad)
                {
                    token.ThrowIfCancellationRequested();
                    var reader = IndexSearcher.GetEntryTermsReader(ids[i], ref page);
                    var id = _documentIdReader.GetTermFor(ids[i]);
                    yield return documentsContext.ReadObject(coraxEntryReader.GetDocument(ref reader), id);
                }

                if ((read = queryMatch.Fill(ids)) == 0)
                    break;
                totalResults.Value += read;
            }

            QueryPool.Return(ids);
            long Skip()
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
            var fieldsInIndex = IndexSearcher.GetFields();
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
            exceptionAggregator.Execute(() => IndexSearcher?.Dispose());
            exceptionAggregator.ThrowIfNeeded();
        }

        internal sealed class GrowableHashSet<TItem>
        {
            private List<HashSet<TItem>> _hashSetsBucket;
            private HashSet<TItem> _newestHashSet;
            private readonly int _maxSizePerCollection;
            private readonly IEqualityComparer<TItem> _comparer;

            public bool HasMultipleHashSets => _hashSetsBucket != null;

            public GrowableHashSet(IEqualityComparer<TItem> comparer = null, int? maxSizePerCollection = null)
            {
                _comparer = comparer;
                _hashSetsBucket = null;
                _maxSizePerCollection = maxSizePerCollection ?? int.MaxValue;
                CreateNewHashSet();
            }

            public bool Add(TItem item)
            {
                if (_newestHashSet!.Count >= _maxSizePerCollection)
                    UnlikelyGrowBuffer();

                if (_hashSetsBucket != null && Contains(item))
                    return false;

                return _newestHashSet.Add(item);
            }

            private void UnlikelyGrowBuffer()
            {
                _hashSetsBucket ??= new();
                _hashSetsBucket.Add(_newestHashSet);
                CreateNewHashSet();
            }

            public bool Contains(TItem item)
            {
                if (_hashSetsBucket != null)
                {
                    foreach (var hashSet in _hashSetsBucket)
                        if (hashSet.Contains(item))
                            return true;
                }

                return _newestHashSet!.Contains(item);
            }

            private void CreateNewHashSet()
            {
                if (_comparer == null)
                    _newestHashSet = new();
                else
                    _newestHashSet = new(_comparer);
            }
        }

        [DoesNotReturn]
        private static void ThrowDistinctOnBiggerCollectionThanInt32()
        {
            throw new NotSupportedInCoraxException($"Corax doesn't support 'Distinct' operation on collection bigger than int32 ({int.MaxValue}).");
        }

        [DoesNotReturn]
        private static void ThrowExplanationsIsNotImplementedInCorax()
        {
            throw new NotSupportedInCoraxException($"{nameof(Corax)} doesn't support {nameof(Explanations)} yet.");
        }
    }
}
