using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Threading;
using Corax;
using Corax.Querying;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Querying.Planning;
using Corax.Utils;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Explanation;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Client.Exceptions.Corax;
using Raven.Server.Documents.Indexes.Debugging;
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
using Sparrow.Server;
using Sparrow.Server.Utils.VxSort;
using Voron;
using Voron.Impl;
using Constants = Raven.Client.Constants;
using CoraxConstants = Corax.Constants;
using IndexSearcher = Corax.Querying.IndexSearcher;
using CoraxSpatialResult = global::Corax.Utils.Spatial.SpatialResult;
using Raven.Server.Documents.Replication.ReplicationItems;
using Sparrow.Server.Logging;
using Voron.Data.CompactTrees;
using Voron.Data.Graphs;
using IndexFieldType = Raven.Server.Documents.Indexes.Debugging.IndexFieldType;

namespace Raven.Server.Documents.Indexes.Persistence.Corax
{
    public class CoraxIndexReadOperation : IndexReadOperationBase
    {
        // PERF: This is a hack to deal with RavenDB-19597. The ArrayPool creates contention under high-request environments.
        // There are 2 ways to avoid this contention, one is to avoid using it altogether, and the other one is separating the pools from
        // the actual executing thread. While the correct approach would be to amp-up the usage of shared buffers (which would make) this
        // hack irrelevant, the complexity it introduces is much greater than what it makes sense to be done at the moment. Therefore,
        // we are building a quick fix that allows us to avoid the locking convoys, and we will defer the real fix to RavenDB-19665.


        [field: ThreadStatic]
        public static ArrayPool<long> QueryPool => field ??= ArrayPool<long>.Create();

        [field: ThreadStatic]
        private static ArrayPool<float> ScorePool => field ??= ArrayPool<float>.Create();

        [field: ThreadStatic]
        private static ArrayPool<CoraxSpatialResult> DistancePool => field ??= ArrayPool<CoraxSpatialResult>.Create();

        protected readonly IndexSearcher IndexSearcher;

        private readonly IndexFieldsMapping _fieldMappings;
        private readonly ByteStringContext _allocator;
        private readonly global::Voron.Impl.LowLevelTransaction _lowLevelTransaction;

        private readonly int _maxNumberOfOutputsPerDocument;

        private TermsReader _documentIdReader;

        public override bool IsSharded => false;

        public CoraxIndexReadOperation(Index index, RavenLogger logger, Transaction readTransaction, QueryBuilderFactories queryBuilderFactories, IndexFieldsMapping fieldsMapping, IndexQueryServerSide query) : base(index, logger, queryBuilderFactories, query)
        {
            _allocator = readTransaction.Allocator;
            _lowLevelTransaction = readTransaction.LowLevelTransaction;
            _fieldMappings = fieldsMapping;
            IndexSearcher = new IndexSearcher(readTransaction, _fieldMappings)
            {
                MaxFacetQueryFilterSizeInBytes = index.Configuration.MaxFacetQueryFilterSize.GetValue(SizeUnit.Bytes),
                PlanCache = (index.IndexPersistence as CoraxIndexPersistence)?.SharedPlanCache ?? new PlanCache(),
            };

            // Attach the per-field HNSW node caches from this transaction's client state and the current
            // fields-with-multiple-terms snapshot to the searcher. The searcher uses the node caches for vector
            // lookups instead of reading through Voron, and the fields snapshot to drive plan selection.
            var vectorCaches = readTransaction.LowLevelTransaction.TryGetClientState(out IndexStateRecord stateRecord)
                               && stateRecord.CoraxVectorState is { Caches: { Count: > 0 } caches }
                ? caches
                : null;
            var fieldsWithMultipleTerms = (index.IndexPersistence as CoraxIndexPersistence)?.FieldsWithMultipleTerms;
            if (vectorCaches != null || fieldsWithMultipleTerms != null)
                IndexSearcher.AttachTransactionCache(vectorCaches, fieldsWithMultipleTerms);

            if (index is { _forTestingPurposes: { CoraxConfiguration: not null } })
                IndexSearcher.SetTestingConfiguration(index._forTestingPurposes.CoraxConfiguration);

            var primaryKey = index.Type.IsMap()
                ? Constants.Documents.Indexing.Fields.DocumentIdFieldName
                : Constants.Documents.Indexing.Fields.ReduceKeyHashFieldName; // map reduce
            _documentIdReader = IndexSearcher.TermsReaderFor(primaryKey);
            _maxNumberOfOutputsPerDocument = index.MaxNumberOfOutputsPerDocument;
        }

        public override long EntriesCount() => IndexSearcher.NumberOfEntries;

        protected interface ISupportsHighlighting
        {
            QueryTimingsScope TimingsScope { get; }
            Dictionary<string, CoraxHighlightingTermIndex> Terms { get; }
            void Initialize(QueryTimingsScope scope);
            void Setup(IndexQueryServerSide query, DocumentsOperationContext context);

            Dictionary<string, Dictionary<string, string[]>> Execute(IndexQueryServerSide query, DocumentsOperationContext context, IndexFieldsMapping fieldMappings,
                ref EntryTermsReader entryReader, Document document, IndexSearcher indexSearcher);
        }

        private struct NoHighlighting : ISupportsHighlighting
        {
            public QueryTimingsScope TimingsScope => null;
            public Dictionary<string, CoraxHighlightingTermIndex> Terms => null;
            public void Initialize(QueryTimingsScope scope) { }
            public void Setup(IndexQueryServerSide query, DocumentsOperationContext context) { }

            public Dictionary<string, Dictionary<string, string[]>> Execute(IndexQueryServerSide query, DocumentsOperationContext context,
                IndexFieldsMapping fieldMappings, ref EntryTermsReader entryReader, Document document, IndexSearcher indexSearcher)
                => null;
        }

        private struct HasHighlighting : ISupportsHighlighting
        {
            private QueryTimingsScope _timingsScope;
            private Dictionary<string, CoraxHighlightingTermIndex> _terms;

            public QueryTimingsScope TimingsScope => _timingsScope;
            public Dictionary<string, CoraxHighlightingTermIndex> Terms => _terms;

            public void Initialize(QueryTimingsScope scope)
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
                                nls = [s.TrimEnd('*').TrimStart('*')];
                                break;
                            case List<string> ls:
                                nls = new string[ls.Count];
                                for (int i = 0; i < ls.Count; i++)
                                    nls[i] = ls[i].TrimEnd('*').TrimStart('*');
                                break;
                            case Tuple<string, string> t2:
                                nls = [t2.Item1.TrimEnd('*').TrimStart('*'), t2.Item2.TrimEnd('*').TrimStart('*')];
                                break;
                            case string[] as1:
                                nls = new string[as1.Length];
                                for (int i = 0; i < as1.Length; i++)
                                    nls[i] = as1[i].TrimEnd('*').TrimStart('*');
                                break;
                            case List<(string Term, bool Exact)> termsWithExact: // In query
                                nls = new string[termsWithExact.Count];
                                for (int i = 0; i < termsWithExact.Count; i++)
                                    nls[i] = termsWithExact[i].Term.TrimEnd('*').TrimStart('*');
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
                IndexFieldsMapping fieldMappings, ref EntryTermsReader entryReader, Document document, IndexSearcher indexSearcher)
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

                        //We have to get analyzer so dynamic field has priority over normal name
                        // We get the field binding to ensure that we are running the analyzer to find the actual tokens.
                        if (fieldMappings.TryGetByFieldName(allocator, fieldDescription.DynamicFieldName ?? fieldDescription.FieldName, out _) == false)
                            continue;

                        // We will get the actual tokens dictionary for this field. If it exists, we get it immediately, if not we create
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
                        if (tokensDictionary.TryGetValue(key, out string[] _))
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
            private LowLevelTransaction _llt;
            private Index _index;
            private IndexQueryServerSide _query;
            private IndexSearcher _searcher;
            private IndexFieldsMapping _fieldsMapping;
            private IQueryResultRetriever _retriever;

            private GrowableHashSet<UnmanagedSpan> _alreadySeenDocumentKeysInPreviousPage;
            private GrowableHashSet<ulong> _alreadySeenProjections;
            public long QueryStart;
            private TermsReader _documentIdReader;
            private bool _canPerformPaginationBasedOnEntriesIds;

            public void Initialize(LowLevelTransaction llt, Index index, IndexQueryServerSide query, IndexSearcher searcher, TermsReader documentIdReader, IndexFieldsMapping fieldsMapping, IQueryResultRetriever retriever)
            {
                _llt = llt;
                _index = index;
                _query = query;
                _searcher = searcher;

                _fieldsMapping = fieldsMapping;
                _retriever = retriever;
                _documentIdReader = documentIdReader;

                QueryStart = _query.Start;

                _canPerformPaginationBasedOnEntriesIds = searcher.EntryIdPaginationSupportStatus == EntryIdPaginationSupportStatus.Supported;

                if (_canPerformPaginationBasedOnEntriesIds == false)
                    _alreadySeenDocumentKeysInPreviousPage = new(UnmanagedSpanComparer.Instance);
            }

            public long RegisterDuplicates<TProjection>(ref TProjection hasProjection, long currentIdx, Span<long> ids, CancellationToken token)
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
                else
                    return 0; // we left it behind, so we are going to continue going for 0. 

                var distinctIds = ids.Slice(0, (int)limit);

                if (hasProjection.IsProjection == false)
                {
                    if (_canPerformPaginationBasedOnEntriesIds == false)
                    {
                        Sort.Run(distinctIds);
                        while (_documentIdReader.GetAllTermsFromSet(distinctIds, out var termsSet) is var read and > 0)
                        {
                            foreach (var key in termsSet)
                                _alreadySeenDocumentKeysInPreviousPage.Add(key);
                            distinctIds = distinctIds[read..];
                        }
                    }

                    return limit;
                }

                using var _ = _llt.AcquireCompactKey(out var existingKey);

                if (typeof(TDistinct) == typeof(HasDistinct))
                {
                    _alreadySeenProjections ??= new();

                    var retriever = _retriever;

                    Page page = default;
                    foreach (var id in distinctIds)
                    {
                        var reader = _searcher.GetEntryTermsReader(id, ref page, existingKey);

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
                if (hasProjection.IsProjection)
                    return true;

                if (_canPerformPaginationBasedOnEntriesIds == false)
                    return _alreadySeenDocumentKeysInPreviousPage.Add(identity);

                return true;
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

        private interface ISupportsQueryFilter : IDisposable
        {
            FilterResult Apply(ref RetrieverInput input, string key);
        }

        private readonly struct NoQueryFilter : ISupportsQueryFilter
        {
            public void Dispose() { }

            public FilterResult Apply(ref RetrieverInput input, string key) => FilterResult.Accepted;
        }

        private readonly struct HasQueryFilter([NotNull] QueryFilter filter) : ISupportsQueryFilter
        {
            public void Dispose()
            {
                filter.Dispose();
            }

            public FilterResult Apply(ref RetrieverInput input, string key) => filter.Apply(ref input, key);
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

        private (SortingDataTransfer SortingData, bool HasSortByDistance) SetupSortingData(IndexQueryServerSide query, QueryBuilderParameters builderParams, IQueryMatch queryMatch, int bufferSize)
        {
            var hasOrderByDistance = query.Metadata.OrderBy is [{ OrderingType: OrderByFieldType.Distance }, ..] && _index.Configuration.CoraxIncludeSpatialDistance;

            SortingDataTransfer sortingData = default;
            if (builderParams.HasBoost || hasOrderByDistance)
            {
                sortingData = new SortingDataTransfer
                {
                    ScoresBuffer = builderParams.NeedsScoresBuffer()
                        ? ScorePool.Rent(bufferSize)
                        : null,
                    DistancesBuffer = _index.Configuration.CoraxIncludeSpatialDistance && hasOrderByDistance
                        ? DistancePool.Rent(bufferSize)
                        : null
                };

                if (queryMatch is IRequireSortingDataTransfer s)
                    s.SetSortingDataTransfer(sortingData);
            }

            return (sortingData, hasOrderByDistance);
        }

        private IEnumerable<QueryResult> QueryInternal<THighlighting, TQueryFilter, THasProjection, TDistinct>(
                    IndexQueryServerSide query, QueryTimingsScope queryTimings, FieldsToFetch fieldsToFetch,
                    Reference<long> totalResults, Reference<long> skippedResults, Reference<long> scannedDocuments,
                    IQueryResultRetriever retriever, DocumentsOperationContext documentsContext,
                    QueryTimeScope queryTime, CancellationToken token)
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
            var llt = documentsContext.Transaction.InnerTransaction.LowLevelTransaction;
            identityTracker.Initialize(llt, _index, query, IndexSearcher, _documentIdReader, _fieldMappings, retriever);

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

            using var scope = documentsContext.Transaction.InnerTransaction.LowLevelTransaction.AcquireCompactKey(out var existingKey);

            THasProjection hasProjections = default;
            THighlighting highlightings = default;
            highlightings.Initialize(queryTimings);

            long docsToLoad = pageSize;
            bool runQuery = true;
            // Loop-invariant: depends only on index type / fields / query.
            bool willAlwaysIncludeInResults = WillAlwaysIncludeInResults(_index.Type, fieldsToFetch, query);
            
            // Reuse a single CompactKey across the whole result loop. EntryTermsReader's Set() restarts the key arena per entry, so reuse is safe and bounded.
            using var entryKeyScope = new CompactKeyCacheScope(_lowLevelTransaction); 
            while (runQuery)
            {
                QueryPlanBuilder.CompiledQuery compileResult;
                // Exact total known from O(1) metadata for single-posting / all-entries plans - to avoid counting throught them
                long knownExactTotal = -1;
                var coraxScope = queryTimings?.For(nameof(QueryTimingsScope.Names.Corax), start: false);
                using (coraxScope?.Start())
                {
                    TransactionOperationContext serverContext = null;
                    using var _ = query.Metadata.HasCmpXchg ? documentsContext.DocumentDatabase.ServerStore.ContextPool.AllocateOperationContext(out serverContext) : null;
                    using var __ = serverContext?.OpenReadTransaction();

                    var builderParameters = new QueryBuilderParameters(IndexSearcher, _allocator, serverContext, documentsContext, query, _index,
                        query.QueryParameters, QueryBuilderFactories, _fieldMappings, fieldsToFetch, highlightings.Terms, (int)take,
                        indexReadOperation: this, token: token, queryTime: queryTime);

                    var planParams = new QueryPlanBuilder.PlanParameters
                    {
                        IndexSearcher = IndexSearcher,
                        Metadata = query.Metadata,
                        QueryParameters = query.QueryParameters,
                        Index = _index,
                        IndexFieldsMapping = _fieldMappings,
                        Allocator = _allocator,
                        HasDynamics = builderParameters.HasDynamics,
                        DynamicFields = builderParameters.DynamicFields,
                        HasBoost = builderParameters.HasBoost
                    };
                    // we scope here the _building of the query, not its execution (below)
                    using (coraxScope?.For(nameof(QueryTimingsScope.Names.Optimizer))?.Start())
                    {
                        compileResult = QueryPlanBuilder.QueryPlanBuilder.BuildSortedQuery(
                            planParams, builderParameters, highlightings.Terms, wantTimings: queryTimings != null,
                            token: token);
                    }

                    if (compileResult.OrderByFields == null && query.Metadata.IsDistinct == false
                                                            && query.Metadata.Query.Filter == null
                                                            && compileResult.QueryMatch is CompiledQueryMatch compiledMatch)
                    {
                        // Single-posting / all-entries plans know their exact total from O(1) metadata
                        knownExactTotal = compiledMatch.Exec.KnownExactTotal;

                        // we try to avoid counting the entire result set, if the count is known, or the caller doesn't care, we can do that
                        if (take > 0 && builderParameters.HasBoost == false && (query.SkipStatistics || knownExactTotal >= 0))
                            compiledMatch.Limit = (int)Math.Min(take * _maxNumberOfOutputsPerDocument, int.MaxValue);
                    }
                    else if (compileResult.QueryMatch is DirectScanMatchBase { KnownExactTotal: >= 0 } directScan)
                    {
                        knownExactTotal = directScan.KnownExactTotal;
                    }
                }

                using var ___ = compileResult;

                highlightings.Setup(query, documentsContext);

                int bufferSize = CoraxBufferSize(IndexSearcher, take, query);
                var ids = QueryPool.Rent(bufferSize);
                using var queryFilter = GetQueryFilterInternal();
                Page page = default;
                totalResults.Value = 0;

                var (sortingData, hasOrderByDistance) = SetupSortingData(query, compileResult.QueryBuilderParams, compileResult.QueryMatch, bufferSize);

                // We don't need to do any processing for the query beyond counting if we are getting a count.
                long totalResultsBefore = totalResults.Value;

                var executeScope = coraxScope?.For(nameof(QueryTimingsScope.Names.Execute), start: false);
                var scoreScope = coraxScope?.For(nameof(QueryTimingsScope.Names.Score), start: false);
                var pagingScope = coraxScope?.For(nameof(QueryTimingsScope.Names.Paging), start: false);
                while (query.IsCountQuery == false || typeof(TDistinct) == typeof(HasDistinct))
                {
                    token.ThrowIfCancellationRequested();

                    // We look for items that hadn't seen before in the case of paging.
                    int read;
                    using (coraxScope?.Start())
                    using (executeScope?.Start())
                        read = compileResult.QueryMatch.Fill(ids);
                    if (read == 0)
                        goto Done;

                    // We need to deal with sorting in Fill, so have to call them on a per batch level
                    if (sortingData.IncludeScores && compileResult.ScoresProducedDuringFill)
                    {
                        var scoresForBatch = sortingData.ScoresBuffer.AsSpan(0, read);
                        scoresForBatch.Fill(Bm25Relevance.InitialScoreValue);
                        using (coraxScope?.Start())
                        using (scoreScope?.Start())
                            compileResult.QueryMatch.Score(ids.AsSpan(0, read), scoresForBatch, 1f);
                    }

                    // If we are going to skip, we've better do it knowing how many we have passed.
                    // After this call the order of ids from 0 to `i` may be changed, and we cannot rely on it (a sorting case).
                    long i;
                    using (coraxScope?.Start())
                    using (pagingScope?.Start())
                    {
                        i = identityTracker.RegisterDuplicates(ref hasProjections, totalResults.Value, ids.AsSpan(0, read), token);
                    }
                    totalResults.Value += read; // important that this is *after* RegisterDuplicates

                    // Now for every document that was selected. document it. 
                    for (; docsToLoad != 0 && i < read; ++i, --docsToLoad)
                    {
                        token.ThrowIfCancellationRequested();

                        long indexEntryId = ids[i];

                        // unless we are going to include no matter what, let's check if we can skip it else.
                        if (willAlwaysIncludeInResults is false)
                        {
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
                        }

                        // Now we know this is a new candidate document to be return therefore, we are going to be getting the
                        // actual data and apply the rest of the filters.

                        float? documentScore = sortingData.IncludeScores ? sortingData.ScoresBuffer[i] : null;
                        CoraxSpatialResult? documentDistance = hasOrderByDistance ? sortingData.DistancesBuffer[i] : null;

                        var key = _documentIdReader.GetTermFor(indexEntryId);
                        EntryTermsReader entryTermsReader = IndexSearcher.GetEntryTermsReader(indexEntryId, ref page, entryKeyScope.Key);
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
                            var qr = CreateQueryResult(ref identityTracker, fetchedDocument.Document, query, documentsContext, ref entryTermsReader, fieldsToFetch, compileResult.OrderByFields, ref highlightings, skippedResults, ref hasProjections, ref markedAsSkipped);
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
                                var qr = CreateQueryResult(ref identityTracker, item, query, documentsContext, ref entryTermsReader, fieldsToFetch, compileResult.OrderByFields, ref highlightings, skippedResults, ref hasProjections, ref markedAsSkipped);
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
                    if (knownExactTotal >= 0) // we already know what the totals are, can just skip the Fill work below
                    {
                        totalResults.Value = knownExactTotal;
                    }
                    else
                    {
                        using (coraxScope?.Start())
                        using (executeScope?.Start())
                        {
                            while(true)
                            {
                                token.ThrowIfCancellationRequested();

                                // Instead of memoizing, we just continue filling the buffer. First, because we don't need to keep the
                                // value or deduplicate at this stage; just to know how many potential matches we have left. Also memoizing
                                // is not supported for SortingMatch.
                                int read = compileResult.QueryMatch.Fill(ids);
                                if (read is 0) break;
                                totalResults.Value += read;
                            }
                        }
                    }
                }

                Done:
                if (queryTimings != null)
                {
                    var inspectionNode = QueryPlanBuilder.QueryPlanBuilder.BuildInspectionGraph(compileResult);
                    queryTimings.SetQueryPlan(inspectionNode);
                }

                ReturnQueryResources(ids, sortingData);


                long sortingMatchTotalResults = compileResult.QueryMatch switch
                {
                    SortingMatch match => match.TotalResults,
                    SortingMultiMatch multiMatch => multiMatch.TotalResults,
                    _ => -1
                };

                if(sortingMatchTotalResults is -1)
                    break; // this is only relevant if we are sorting, since we may have filtered items and need to read more, see: RavenDB-20294

                if (docsToLoad == 0 ||
                    sortingMatchTotalResults == totalResults.Value ||
                    totalResults.Value == totalResultsBefore || // no progress this iteration — match exhausted
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

            TQueryFilter GetQueryFilterInternal()
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

        private static void ReturnQueryResources(long[] ids, SortingDataTransfer sortingData)
        {
            QueryPool.Return(ids);
            if (sortingData.IncludeScores)
                ScorePool.Return(sortingData.ScoresBuffer);
            if (sortingData.IncludeDistances)
                DistancePool.Return(sortingData.DistancesBuffer);
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
                Highlightings = highlightings.Execute(query, documentsContext, _fieldMappings, ref entryReader, document, IndexSearcher),
            };
        }

        public override IEnumerable<QueryResult> Query(IndexQueryServerSide query, QueryTimingsScope queryTimings, FieldsToFetch fieldsToFetch,
            Reference<long> totalResults, Reference<long> skippedResults,
            Reference<long> scannedDocuments, IQueryResultRetriever retriever, DocumentsOperationContext documentsContext, Func<string, SpatialField> getSpatialField,
            QueryTimeScope queryTime, CancellationToken token)
        {
            // We've a chain-like builder here.
            return BuildHighlightings();

            IEnumerable<QueryResult> BuildHighlightings() => query.Metadata.HasHighlightings switch
            {
                true => BuildFilterScript<HasHighlighting>(),
                _ => BuildFilterScript<NoHighlighting>()
            };

            IEnumerable<QueryResult> BuildFilterScript<THighlighting>()
                where THighlighting : struct, ISupportsHighlighting
                => (query.Metadata.FilterScript is null) switch
                {
                    true => BuildProjection<THighlighting, NoQueryFilter>(),
                    _ => BuildProjection<THighlighting, HasQueryFilter>()
                };

            IEnumerable<QueryResult> BuildProjection<THighlighting, TQueryFilter>()
                where THighlighting : struct, ISupportsHighlighting
                where TQueryFilter : struct, ISupportsQueryFilter
                => fieldsToFetch.IsProjection switch
                {
                    true => BuildDistinct<THighlighting, TQueryFilter, HasProjection>(),
                    _ => BuildDistinct<THighlighting, TQueryFilter, NoProjection>()
                };

            IEnumerable<QueryResult> BuildDistinct<THighlighting, TQueryFilter, THasProjection>()
                where THighlighting : struct, ISupportsHighlighting
                where TQueryFilter : struct, ISupportsQueryFilter
                where THasProjection : struct, IHasProjection
                => query.Metadata.IsDistinct switch
                {
                    true => BuildInternalQuery<THighlighting, TQueryFilter, THasProjection, HasDistinct>(),
                    _ => BuildInternalQuery<THighlighting, TQueryFilter, THasProjection, NoDistinct>()
                };

            IEnumerable<QueryResult> BuildInternalQuery<THighlighting, TQueryFilter, THasProjection, TDistinct>()
                where THighlighting : struct, ISupportsHighlighting
                where TQueryFilter : struct, ISupportsQueryFilter
                where THasProjection : struct, IHasProjection
                where TDistinct : struct, IHasDistinct
                => QueryInternal<THighlighting, TQueryFilter, THasProjection, TDistinct>(
                    query, queryTimings, fieldsToFetch,
                    totalResults, skippedResults, scannedDocuments,
                    retriever, documentsContext,
                    queryTime, token);
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
            Reference<long> skippedResults, Reference<long> scannedDocuments, IQueryResultRetriever retriever,
            DocumentsOperationContext documentsContext, Func<string, SpatialField> getSpatialField, QueryTimeScope queryTime, CancellationToken token)
        {
            throw new NotSupportedException($"{nameof(Corax)} does not support intersect queries.");
        }

        public override List<string> Terms(string field, string fromValue, long pageSize, CancellationToken token)
        {
            if (IndexSearcher.TryGetVectorsOfField(field, out var vectorsRetriever))
            {
                List<string> terms = new();
                return TermsInternal(fromValue, pageSize, vectorsRetriever, terms, token);
            }

            if (IndexSearcher.TryGetTermsOfField(IndexSearcher.FieldMetadataBuilder(field), out var termsRetriever))
            {
                SortedSet<string> terms = new(StringComparer.Ordinal);
                return TermsInternal(fromValue, pageSize, termsRetriever, terms, token).ToList();
            }

            return [];
        }

        private TResult TermsInternal<TRetriever, TResult>(string fromValue, long pageSize, TRetriever retriever, TResult results, CancellationToken token)
            where TRetriever : IIndexedTermsRetriever
            where TResult : ICollection<string>
        {
            if (string.IsNullOrEmpty(fromValue) == false)
            {
                Span<byte> fromValueBytes = StringToValue(fromValue);
                while (retriever.GetNextTerm(out var currentTerm) && currentTerm.SequenceEqual(fromValueBytes) == false)
                {
                    token.ThrowIfCancellationRequested();
                }
            }

            while (pageSize > 0 && retriever.GetNextTerm(out var currentTerm))
            {
                token.ThrowIfCancellationRequested();
                results.Add(ValueToString(currentTerm));
                pageSize--;
            }

            return results;

            string ValueToString(ReadOnlySpan<byte> bytes)
            {
                return retriever.Type switch
                {
                    ConvertTo.Base64 => Convert.ToBase64String(bytes),
                    ConvertTo.String => Encodings.Utf8.GetString(bytes),
                    _ => throw new NotSupportedException($"The type {retriever.Type} is not supported.")
                };
            }

            Span<byte> StringToValue(string value)
            {
                return retriever.Type switch
                {
                    ConvertTo.Base64 => Convert.FromBase64String(value),
                    ConvertTo.String => Encodings.Utf8.GetBytes(value),
                    _ => throw new NotSupportedException($"The type {retriever.Type} is not supported.")
                };
            }
        }

        public override IEnumerable<QueryResult> MoreLikeThis(IndexQueryServerSide query, IQueryResultRetriever retriever, DocumentsOperationContext context,
            CancellationToken token)
        {
            IDisposable releaseServerContext = null;
            IDisposable closeServerTransaction = null;
            TransactionOperationContext serverContext = null;
            MoreLikeThisQuery moreLikeThisQuery;
            QueryBuilderParameters builderParameters;

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
                        _fieldMappings, null, null /* allow highlighting? */, global::Corax.Constants.IndexSearcher.TakeAll, indexReadOperation: this, token: token);
                    moreLikeThisQuery = BuildMoreLikeThisQuery(builderParameters, query.Metadata.Query.Where);
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
                _fieldMappings, null, null /* allow highlighting? */, global::Corax.Constants.IndexSearcher.TakeAll, indexReadOperation: this, token: token);
            using var mlt = new RavenRavenMoreLikeThis(builderParameters, options);
            long? baseDocId = null;

            if (moreLikeThisQuery.BaseDocument == null)
            {
                Span<long> docsIds = stackalloc long[16];

                var baseDocQuery = moreLikeThisQuery.BaseDocumentQuery;
                using (baseDocQuery as IDisposable)
                {
                    // get the current Lucene docid for the given RavenDB doc ID
                    if (baseDocQuery.Fill(docsIds) == 0)
                        throw new InvalidOperationException("Given filtering expression did not yield any documents that could be used as a base of comparison");

                    //What if we've got multiple items?
                    baseDocId = docsIds[0];
                }
            }

            if (stopWords != null)
                mlt.SetStopWords(stopWords);

            string[] fieldNames;
            if (options.Fields is { Length: > 0 })
                fieldNames = options.Fields;
            else
            {
                fieldNames = new string[_fieldMappings.Count];
                var index = 0;
                foreach (var binding in _fieldMappings)
                {
                    if (binding.FieldNameAsString is Constants.Documents.Indexing.Fields.DocumentIdFieldName or Constants.Documents.Indexing.Fields.SourceDocumentIdFieldName or Constants.Documents.Indexing.Fields.ReduceKeyHashFieldName)
                        continue;
                    fieldNames[index++] = binding.FieldNameAsString;

                }

                if (index < fieldNames.Length)
                    Array.Resize(ref fieldNames, index);
            }

            mlt.SetFieldNames(fieldNames);

            var pageSize = CoraxBufferSize(IndexSearcher, query.PageSize, query);

            // MoreLikeThis returns an array of term matches. We OR them into a bitmap,
            // then AND with the filter query if present. Bitmap OR is inherently
            // deduplicated — no DeduplicationMatch needed.
            IQueryMatch[] mltTerms;
            if (baseDocId.HasValue)
            {
                mltTerms = mlt.Like(baseDocId.Value);
            }
            else
            {
                using (var blittableJson = ParseJsonStringIntoBlittable(moreLikeThisQuery.BaseDocument, context))
                    mltTerms = mlt.Like(blittableJson);
            }

            // Materialize into bitmap via OR
            Voron.Data.RoaringBitmaps.RoaringBitmap mltBitmapData = new(_allocator);
            long[] ids = null;
            Voron.Data.RoaringBitmaps.RoaringBitmapIterator mltIterator = default;
            try
            {
                Span<long> fillBuf = stackalloc long[4096];
                foreach (var termMatch in mltTerms)
                {
                    int termRead;
                    while ((termRead = termMatch.Fill(fillBuf)) > 0)
                        mltBitmapData.AddRange(fillBuf[..termRead]);
                }

                // AND with filter query if present
                if (moreLikeThisQuery.FilterQuery != null && moreLikeThisQuery.FilterQuery is AllEntriesMatch == false)
                {
                    var filterMatch = moreLikeThisQuery.FilterQuery;
                    Voron.Data.RoaringBitmaps.RoaringBitmap filterBitmapData = new(_allocator);
                    try
                    {
                        int filterRead;
                        while ((filterRead = filterMatch.Fill(fillBuf)) > 0)
                            filterBitmapData.AddRange(fillBuf[..filterRead]);
                        mltBitmapData.AndWith(ref filterBitmapData);
                    }
                    finally
                    {
                        filterBitmapData.Dispose();
                        (filterMatch as IDisposable)?.Dispose();
                    }
                }

                mltBitmapData.PrepareForReading();
                mltIterator = mltBitmapData.GetIterator();

                var ravenIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                ids = QueryPool.Rent(pageSize);
                long returnedDocs = 0;
                long skippedDocs = 0;
                Page page = default;
                int read;
                while ((read = mltIterator.Fill(ref mltBitmapData, ids.AsSpan())) != 0)
                {
                    for (int i = 0; i < read; i++)
                    {
                        if (returnedDocs >= query.Limit)
                            yield break;
                        
                        var hit = ids[i];
                        token.ThrowIfCancellationRequested();

                        if (hit == baseDocId)
                            continue;
                        
                        var id = _documentIdReader.GetTermFor(hit);
                        if (ravenIds.Add(id) == false)
                            continue;

                        if (skippedDocs < query.Start)
                        {
                            skippedDocs++;
                            continue;
                        }
                        
                        var termsReader = IndexSearcher.GetEntryTermsReader(hit, ref page);
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
            }
            finally
            {
                if (ids != null)
                    QueryPool.Return(ids);
                mltIterator.Dispose();
                mltBitmapData.Dispose();
            }
        }

        public string GetDocumentIdFor(long entryId)
        {
            return _documentIdReader.GetTermFor(entryId);
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

            var builderParameters = new QueryBuilderParameters(IndexSearcher, _allocator, null,
                documentsContext, query, _index, query.QueryParameters, QueryBuilderFactories,
                _fieldMappings, null, null, (int)take,
                indexReadOperation: this, token: token);

            // Route through the sorted pipeline so the raw-entries view honors ORDER BY, matching Lucene.
            using var compileResult = QueryPlanBuilder.QueryPlanBuilder.BuildSortedQuery(
                new QueryPlanBuilder.PlanParameters
                {
                    IndexSearcher = IndexSearcher,
                    Metadata = query.Metadata,
                    QueryParameters = query.QueryParameters,
                    Index = _index,
                    IndexFieldsMapping = _fieldMappings,
                    Allocator = _allocator,
                    HasDynamics = builderParameters.HasDynamics,
                    DynamicFields = builderParameters.DynamicFields,
                    HasBoost = builderParameters.HasBoost
                }, builderParameters, highlightingTerms: null, wantTimings: false, token);

            IQueryMatch queryMatch = compileResult.QueryMatch;

            var ids = QueryPool.Rent(CoraxBufferSize(IndexSearcher, take, query));

            var (sortingData, _) = SetupSortingData(query, compileResult.QueryBuilderParams, queryMatch, ids.Length);

            // docsToLoad is the logical page budget (how many entries to yield), and NOT the buffer-rental size.
            int docsToLoad = pageSize >= int.MaxValue ? int.MaxValue : (int)pageSize;
            using var coraxEntryReader = new CoraxIndexedEntriesReader(documentsContext, IndexSearcher);

            int read;
            long i = Skip();
            Page page = default;
            var alreadySeenDocuments = new HashSet<long>();

            while (true)
            {
                token.ThrowIfCancellationRequested();
                for (; docsToLoad != 0 && i < read; ++i, --docsToLoad)
                {
                    var coraxInternalEntryId = ids[i];
                    if (alreadySeenDocuments.Add(coraxInternalEntryId) == false)
                        continue;

                    token.ThrowIfCancellationRequested();
                    var reader = IndexSearcher.GetEntryTermsReader(coraxInternalEntryId, ref page);
                    var id = _documentIdReader.GetTermFor(coraxInternalEntryId);

                    var dynamicJsonValue = coraxEntryReader.GetDocument(ref reader);
                    yield return documentsContext.ReadObject(dynamicJsonValue, id);
                }

                if ((read = queryMatch.Fill(ids)) == 0)
                    break;
                totalResults.Value += read;
            }

            ReturnQueryResources(ids, sortingData);
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

        public override HashSet<FieldDebugInfo> GetEntriesFields(ICollection<string> unknownTypeStaticFields)
        {
            var fields = new HashSet<FieldDebugInfo>();
            foreach (var staticField in unknownTypeStaticFields)
            {
                var termType = IndexSearcher.IsVectorField(staticField) ?
                    IndexedValueType.Vector
                    : IndexedValueType.Term;

                fields.Add(new FieldDebugInfo(staticField, IndexFieldType.Static, termType));
            }

            var fieldsInIndex = IndexSearcher.GetFields();
            foreach (var fieldName in fieldsInIndex)
            {
                if (fields.Select(x => x.Name).Contains(fieldName))
                    continue;

                if (IsDynamicFieldKnownAsStatic(fieldName))
                    continue;

                var termType = IndexSearcher.IsVectorField(fieldName) ?
                    IndexedValueType.Vector
                    : IndexedValueType.Term;

                fields.Add(new(fieldName, IndexFieldType.Dynamic, termType));
            }

            return fields;
        }

        public override void Dispose()
        {
            base.Dispose();

            var exceptionAggregator = new ExceptionAggregator($"Could not dispose {nameof(CoraxIndexReadOperation)} of {_index.Name}");
            exceptionAggregator.Execute(() => IndexSearcher?.Dispose());
            exceptionAggregator.ThrowIfNeeded();
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

        private static MoreLikeThisQuery BuildMoreLikeThisQuery(QueryBuilderParameters builderParameters, QueryExpression whereExpression)
        {
            using (CultureHelper.EnsureInvariantCulture())
            {
                var indexSearcher = builderParameters.IndexSearcher;
                var metadata = builderParameters.Metadata;
                var queryParameters = builderParameters.QueryParameters;
                var context = builderParameters.DocumentsContext;

                var moreLikeThisExpression = QueryBuilderHelper.FindMoreLikeThisExpression(whereExpression);
                if (moreLikeThisExpression == null)
                    throw new InvalidOperationException("Query does not contain MoreLikeThis method expression");

                BlittableJsonReaderObject options = null;
                if (moreLikeThisExpression.Arguments.Count == 2)
                {
                    var value = QueryBuilderHelper.GetValue(metadata.Query, metadata, queryParameters, moreLikeThisExpression.Arguments[1], allowObjectsInParameters: true);
                    if (value.Type == ValueTokenType.String)
                        options = ParseJsonStringIntoBlittable(QueryBuilderHelper.GetValueAsString(value.Value), context);
                    else
                        options = value.Value as BlittableJsonReaderObject;
                }

                string baseDocument = null;
                IQueryMatch baseDocumentQuery = null;
                var firstArgument = moreLikeThisExpression.Arguments[0];
                if (firstArgument is BinaryExpression be)
                {
                    // moreLikeThis(id() = 'datas/4-A', ...) — build a query from just the
                    // inner binary expression (not the full WHERE which wraps it in moreLikeThis).
                    baseDocumentQuery = QueryPlanBuilder.QueryPlanBuilder.BuildQueryForMoreLikeThis(builderParameters, moreLikeThisExpression, be);
                }
                else
                {
                    // Value argument: either a boolean (true → all entries) or a document ID string.
                    // moreLikeThis(true, ...) → compare against all entries
                    // moreLikeThis('datas/4-A', ...) → baseDocument is loaded from DocumentsStorage later
                    var firstArgumentValue = QueryBuilderHelper.GetValueAsString(QueryBuilderHelper.GetValue(metadata.Query, metadata, queryParameters, firstArgument).Value);
                    if (bool.TryParse(firstArgumentValue, out var firstArgumentBool))
                    {
                        baseDocumentQuery = firstArgumentBool
                            ? indexSearcher.AllEntries()
                            : indexSearcher.EmptyMatch();
                    }
                    else
                    {
                        // Document ID as a string — the MoreLikeThis reader loads the document
                        // and extracts terms from it (not via index lookup).
                        baseDocument = firstArgumentValue;
                    }
                }

                var filterQuery = BuildCompiledQueryMatch(builderParameters);

                return new MoreLikeThisQuery
                {
                    BaseDocument = baseDocument,
                    BaseDocumentQuery = baseDocumentQuery,
                    FilterQuery = filterQuery,
                    Options = options
                };
            }
        }

        private static IQueryMatch BuildCompiledQueryMatch(QueryBuilderParameters builderParameters)
        {
            var planParams = new QueryPlanBuilder.PlanParameters
            {
                IndexSearcher = builderParameters.IndexSearcher,
                Metadata = builderParameters.Query.Metadata,
                QueryParameters = builderParameters.QueryParameters,
                Index = builderParameters.Index,
                IndexFieldsMapping = builderParameters.IndexFieldsMapping,
                Allocator = builderParameters.Allocator,
                HasDynamics = builderParameters.HasDynamics,
                DynamicFields = builderParameters.DynamicFields,
                HasBoost = builderParameters.HasBoost
            };
            return QueryPlanBuilder.QueryPlanBuilder.BuildFilterMatch(
                planParams, builderParameters, highlightingTerms: null, wantTimings: false, builderParameters.Token);
        }
    }
}
