using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Threading;
using Amazon.SQS.Model;
using Corax;
using Corax.Mappings;
using Corax.Queries;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Explanation;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Queries;
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
using Voron.Impl;
using CoraxConstants = Corax.Constants;
using IndexSearcher = Corax.IndexSearcher;

namespace Raven.Server.Documents.Indexes.Persistence.Corax
{
    public class CoraxIndexReadOperation : IndexReadOperationBase
    {
        // PERF: This is a hack in order to deal with RavenDB-19597. The ArrayPool creates contention under high requests environments.
        // There are 2 ways to avoid this contention, one is to avoid using it altogether and the other one is separating the pools from
        // the actual executing thread. While the correct approach would be to amp-up the usage of shared buffers (which would make) this
        // hack irrelevant, the complexity it introduces is much greater than what it make sense to be done at the moment. Therefore, 
        // we are building a quick fix that allow us to avoid the locking convoys and we will defer the real fix to RavenDB-19665. 
        [ThreadStatic] private static ArrayPool<long> _queryPool;

        public static ArrayPool<long> QueryPool
        {
            get
            {
                _queryPool ??= ArrayPool<long>.Create();
                return _queryPool;
            }
        }

        private readonly IndexFieldsMapping _fieldMappings;
        private readonly IndexSearcher _indexSearcher;
        private readonly ByteStringContext _allocator;
        private readonly int _maxNumberOfOutputsPerDocument;


        public CoraxIndexReadOperation(Index index, Logger logger, Transaction readTransaction, QueryBuilderFactories queryBuilderFactories, IndexFieldsMapping fieldsMapping, IndexQueryServerSide query) : base(index, logger, queryBuilderFactories, query)
        {
            _allocator = readTransaction.Allocator;
            _fieldMappings = fieldsMapping;
            _indexSearcher = new IndexSearcher(readTransaction, _fieldMappings);
            _maxNumberOfOutputsPerDocument = index.MaxNumberOfOutputsPerDocument;
        }

        public override long EntriesCount() => _indexSearcher.NumberOfEntries;


        private interface ISupportsHighlighting
        {
            QueryTimingsScope TimingsScope { get; }
            Dictionary<string, CoraxHighlightingTermIndex> Terms { get; }
            void Initialize(IndexQueryServerSide query, QueryTimingsScope scope);
            void Setup(IndexQueryServerSide query, DocumentsOperationContext context);

            Dictionary<string, Dictionary<string, string[]>> Execute(IndexQueryServerSide query, DocumentsOperationContext context, IndexFieldsMapping fieldMappings, Document document);
        }

        private struct NoHighlighting : ISupportsHighlighting
        {
            public QueryTimingsScope TimingsScope => null;
            public Dictionary<string, CoraxHighlightingTermIndex> Terms => null;
            public void Initialize(IndexQueryServerSide query, QueryTimingsScope scope) { }
            public void Setup(IndexQueryServerSide query, DocumentsOperationContext context) { }

            public Dictionary<string, Dictionary<string, string[]>> Execute(
                IndexQueryServerSide query, DocumentsOperationContext context,
                IndexFieldsMapping fieldMappings, Document document)
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

                        var fieldName =
                            query.Metadata.IsDynamic
                                ? AutoIndexField.GetHighlightingAutoIndexFieldName(highlighting.Field.Value)
                                : highlighting.Field.Value;

                        if (highlightingTerms.TryGetValue(fieldName, out var termIndex) == false)
                        {
                            // the case when we have to create MapReduce highlighter
                            termIndex = new()
                            {
                                FieldName = highlighting.Field.Value,
                                DynamicFieldName = AutoIndexField.GetHighlightingAutoIndexFieldName(highlighting.Field.Value),
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

            public Dictionary<string, Dictionary<string, string[]>> Execute(IndexQueryServerSide query, DocumentsOperationContext context, IndexFieldsMapping fieldMappings, Document document)
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
                        if (highlightingTerms.TryGetValue(fieldName, out var fieldDescription) == false)
                            continue;

                        //We have to get analyzer so dynamic field have priority over normal name
                        // We get the field binding to ensure that we are running the analyzer to find the actual tokens.
                        if (fieldMappings.TryGetByFieldName(allocator, fieldDescription.DynamicFieldName ?? fieldDescription.FieldName, out var fieldBinding) == false)
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
                                        BlittableJsonToken.String => ((LazyStringValue)property.Value).ToString(CultureInfo.InvariantCulture),
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

                    return highlightings;
                }
            }
        }

        private interface IHasDistinct
        {
        }

        private struct NoDistinct : IHasDistinct { }

        private struct HasDistinct : IHasDistinct { }


        // Even if there are no distinct statements we have to be sure that we are not including
        // documents that we have already included during this request. 
        private struct IdentityTracker<TDistinct> where TDistinct : struct, IHasDistinct
        {
            private Index _index;
            private IndexQueryServerSide _query;
            private IndexSearcher _searcher;
            private IndexFieldsMapping _fieldsMapping;
            private IQueryResultRetriever _retriever;

            private bool _isMap;

            private HashSet<UnmanagedSpan> _alreadySeenDocumentKeysInPreviousPage;
            private HashSet<ulong> _alreadySeenProjections;
            public int QueryStart;

            public void Initialize(Index index, IndexQueryServerSide query, IndexSearcher searcher, IndexFieldsMapping fieldsMapping, FieldsToFetch fieldsToFetch, IQueryResultRetriever retriever)
            {
                _index = index;
                _query = query;
                _searcher = searcher;
                _fieldsMapping = fieldsMapping;
                _retriever = retriever;

                _alreadySeenDocumentKeysInPreviousPage = new(UnmanagedSpanComparer.Instance);
                QueryStart = _query.Start;
                _isMap = index.Type.IsMap();
            }

            public int RegisterDuplicates<TProjection>(ref TProjection hasProjection, int currentIdx, ReadOnlySpan<long> ids, CancellationToken token)
                where TProjection : struct, IHasProjection
            {
                // From now on, we know we will try to skip duplicates.
                int limit = 0;

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
                // else we left it behind, so we are going to continue going for 0. 
                else
                    return 0;

                var distinctIds = ids.Slice(0, limit);
                
                if (_isMap && hasProjection.IsProjection == false)
                {
                    // Assumptions: we're in Map, so that mean we have ID of the doc saved in the tree. So we want to keep track what we returns
                    foreach (var id in distinctIds)
                    {
                        var key = _searcher.GetRawIdentityFor(id);
                        _alreadySeenDocumentKeysInPreviousPage.Add(key);
                    }

                    return limit;
                }

                if (typeof(TDistinct) == typeof(HasDistinct))
                {
                    _alreadySeenProjections ??= new();

                    var retriever = _retriever;
                    foreach (var id in distinctIds)
                    {
                        var coraxEntry = _searcher.GetReaderAndIdentifyFor(id, out var key);
                        var retrieverInput = new RetrieverInput(_searcher, _fieldsMapping, coraxEntry, key, _index.IndexFieldsPersistence);
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
                if (hasProjection.IsProjection && _alreadySeenDocumentKeysInPreviousPage.Contains(identity))
                    return false;

                if (hasProjection.IsProjection == false)
                {
                    if (_alreadySeenDocumentKeysInPreviousPage.Add(identity) == false)
                        return false;
                }

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

        private interface IHasProjection
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
            return fieldsToFetch.IsDistinct || indexType.IsMapReduce() || query.SkipDuplicateChecking;
        }

        private IEnumerable<QueryResult> QueryInternal<THighlighting, TQueryFilter, THasProjection, TDistinct>(
                    IndexQueryServerSide query, QueryTimingsScope queryTimings, FieldsToFetch fieldsToFetch,
                    Reference<int> totalResults, Reference<int> skippedResults, Reference<int> scannedDocuments,
                    IQueryResultRetriever retriever, DocumentsOperationContext documentsContext,
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
            identityTracker.Initialize(_index, query, _indexSearcher, _fieldMappings, fieldsToFetch, retriever);

            int pageSize = query.PageSize;
            
            if (query.Metadata.HasExplanations)
                throw new NotImplementedException($"{nameof(Corax)} doesn't support {nameof(Explanations)} yet.");
            
            int take = pageSize + query.Start;
            if (take > _indexSearcher.NumberOfEntries || fieldsToFetch.IsDistinct)
                take = CoraxConstants.IndexSearcher.TakeAll;
            
            bool isDistinctCount = query.PageSize == 0 && typeof(TDistinct) == typeof(HasDistinct);
            if (isDistinctCount)
            {
                pageSize = int.MaxValue;
                take = CoraxConstants.IndexSearcher.TakeAll;
            }

            THasProjection hasProjections = default;
            THighlighting highlightings = default;
            highlightings.Initialize(query, queryTimings);

            int docsToLoad = pageSize;
            bool runQuery = true;
            while (runQuery)
            {
                IQueryMatch queryMatch;

                using (queryTimings?.For(nameof(QueryTimingsScope.Names.Corax), start: false)?.Start())
                {
                    var builderParameters = new CoraxQueryBuilder.Parameters(_indexSearcher, _allocator, serverContext: null, documentsContext: null, query, _index,
                        query.QueryParameters, QueryBuilderFactories, _fieldMappings, fieldsToFetch, highlightings.Terms, take);

                    if ((queryMatch = CoraxQueryBuilder.BuildQuery(builderParameters)) is null)
                        yield break;
                }

                highlightings.Setup(query, documentsContext);

                int bufferSize = CoraxBufferSize(_indexSearcher, take, query);
                var ids = QueryPool.Rent(bufferSize);


                using var queryFilter = GetQueryFilter();

                bool willAlwaysIncludeInResults = WillAlwaysIncludeInResults(_index.Type, fieldsToFetch, query);
                totalResults.Value = 0;
                while (true)
                {
                    token.ThrowIfCancellationRequested();

                    // We look for items that was haven't seen before in the case of paging. 
                    int read = queryMatch.Fill(ids);
                    if (read == 0)
                        break;

                    // If we are going to skip, we've better do it knowing how many we have passed. 
                    int i = identityTracker.RegisterDuplicates(ref hasProjections, totalResults.Value, ids.AsSpan(0, read), token);
                    totalResults.Value += read; // important that this is *after* RegisterDuplicates

                    // Now for every document that was selected. document it. 
                    for (; docsToLoad != 0 && i < read; ++i, --docsToLoad)
                    {
                        token.ThrowIfCancellationRequested();

                        // If we are going to include no matter what, lets skip everything else.
                        if (willAlwaysIncludeInResults)
                            goto Include;

                        // Ok, we will need to check for duplicates, then we will have to work. In some cases (like TimeSeries) we don't "have" unique identifier so we skip checking.
                        var identityExists = retriever.TryGetKeyCorax(_indexSearcher, ids[i], out var rawIdentity);

                        // If we have figured out that this document identity has already been seen, we are skipping it.
                        if (identityExists && identityTracker.ShouldIncludeIdentity(ref hasProjections, rawIdentity) == false)
                        {
                            docsToLoad++;
                            skippedResults.Value++;
                            continue;
                        }

                        // Now we know this is a new candidate document to be return therefore, we are going to be getting the
                        // actual data and apply the rest of the filters. 
                        Include:
                        var retrieverInput = new RetrieverInput(_indexSearcher, _fieldMappings, _indexSearcher.GetReaderAndIdentifyFor(ids[i], out var key), key,
                            _index.IndexFieldsPersistence);

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
                            var qr = GetQueryResult(ref identityTracker, fetchedDocument.Document, ref markedAsSkipped);
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
                                var qr = GetQueryResult(ref identityTracker, item, ref markedAsSkipped);
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

                    // No need to continue filling buffers as there are no more docs to load.
                    if (docsToLoad <= 0)
                        break;
                }

                QueryPool.Return(ids);

                if (queryMatch is not SortingMatch sm)
                    break; // this is only relevant if we are sorting, since we may have filtered items and need to read more, see: RavenDB-20294

                if (docsToLoad == 0 ||
                    sm.TotalResults == totalResults.Value ||
                    scannedDocuments.Value >= query.FilterLimit)
                {
                    totalResults.Value = (int)Math.Min(sm.TotalResults, int.MaxValue);
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
                else if (typeof(TQueryFilter) == typeof(HasQueryFilter))
                {
                    return (TQueryFilter)(object)new HasQueryFilter(
                        new QueryFilter(_index, query, documentsContext, skippedResults, scannedDocuments, retriever, queryTimings)
                    );
                }
                else
                    throw new UnsupportedOperationException($"The type {typeof(TQueryFilter)} is not supported.");
            }

            QueryResult GetQueryResult(ref IdentityTracker<TDistinct> tracker, Document document, ref bool markedAsSkipped)
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
                    Highlightings = highlightings.Execute(query, documentsContext, _fieldMappings, document),
                };
            }
        }

        public override IEnumerable<QueryResult> Query(IndexQueryServerSide query, QueryTimingsScope queryTimings, FieldsToFetch fieldsToFetch,
            Reference<int> totalResults, Reference<int> skippedResults,
            Reference<int> scannedDocuments, IQueryResultRetriever retriever, DocumentsOperationContext documentsContext, Func<string, SpatialField> getSpatialField,
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

        private static void SetupHighlighter(IndexQueryServerSide query, JsonOperationContext context, Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms)
        {
            foreach (var term in highlightingTerms)
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

        public override IEnumerable<QueryResult> IntersectQuery(IndexQueryServerSide query, FieldsToFetch fieldsToFetch, Reference<int> totalResults,
            Reference<int> skippedResults, Reference<int> scannedDocuments, IQueryResultRetriever retriever,
            DocumentsOperationContext documentsContext, Func<string, SpatialField> getSpatialField, CancellationToken token)
        {
            throw new NotImplementedException($"{nameof(Corax)} does not support intersect queries.");
        }

        public override HashSet<string> Terms(string field, string fromValue, long pageSize, CancellationToken token)
        {
            HashSet<string> results = new();

            if (_indexSearcher.TryGetTermsOfField(_indexSearcher.FieldMetadataBuilder(field), out var terms) == false)
                return results;

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
                    builderParameters = new(_indexSearcher, _allocator, serverContext, context, query, _index, query.QueryParameters, QueryBuilderFactories,
                        _fieldMappings, null, null /* allow highlighting? */, CoraxQueryBuilder.TakeAll, null);
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

            builderParameters = new(_indexSearcher, _allocator, null, context, query, _index, query.QueryParameters, QueryBuilderFactories,
                _fieldMappings, null, null /* allow highlighting? */, CoraxQueryBuilder.TakeAll, null);
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

            var pageSize = CoraxBufferSize(_indexSearcher, query.PageSize, query);

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

                    var retrieverInput = new RetrieverInput(_indexSearcher, _fieldMappings, reader, id, _index.IndexFieldsPersistence);
                    var result = retriever.Get(ref retrieverInput, token);
                    if (result.Document != null)
                    {
                        yield return new QueryResult { Result = result.Document };
                    }
                    else if (result.List != null)
                    {
                        foreach (Document item in result.List)
                        {
                            yield return new QueryResult { Result = item };
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
            var builderParameters = new CoraxQueryBuilder.Parameters(_indexSearcher, _allocator, null, null, query, _index, null, null, _fieldMappings, null, null, -1, null);
            if ((queryMatch = CoraxQueryBuilder.BuildQuery(builderParameters)) is null)
                yield break;

            var ids = QueryPool.Rent(CoraxBufferSize(_indexSearcher, take, query));
            int docsToLoad = CoraxBufferSize(_indexSearcher, pageSize, query);
            using var coraxEntryReader = new CoraxIndexedEntriesReader(_indexSearcher, _fieldMappings);
            int read;
            int i = Skip();
            while (true)
            {
                token.ThrowIfCancellationRequested();
                for (; docsToLoad != 0 && i < read; ++i, --docsToLoad)
                {
                    token.ThrowIfCancellationRequested();
                    var reader = _indexSearcher.GetReaderAndIdentifyFor(ids[i], out var id);
                    yield return documentsContext.ReadObject(coraxEntryReader.GetDocument(ref reader), id);
                }

                if ((read = queryMatch.Fill(ids)) == 0)
                    break;
                totalResults.Value += read;
            }

            QueryPool.Return(ids);
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
