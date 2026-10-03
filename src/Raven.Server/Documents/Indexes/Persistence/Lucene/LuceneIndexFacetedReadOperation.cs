using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Facets;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Exceptions;
using Raven.Server.Indexing;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public sealed class LuceneIndexFacetedReadOperation : IndexFacetReadOperationBase
    {
        private readonly IndexSearcher _searcher;
        private readonly IDisposable _releaseReadTransaction;
        private readonly LuceneRavenPerFieldAnalyzerWrapper _analyzer;
        private readonly IDisposable _releaseSearcher;

        private readonly IState _state;

        public LuceneIndexFacetedReadOperation(Index index,
            IndexDefinitionBaseServerSide indexDefinition,
            LuceneVoronDirectory directory,
            LuceneIndexSearcherHolder searcherHolder,
            QueryBuilderFactories queryBuilderFactories,
            Transaction readTransaction,
            DocumentDatabase documentDatabase)
            : base(index, queryBuilderFactories, LoggingSource.Instance.GetLogger<LuceneIndexFacetedReadOperation>(documentDatabase.Name))
        {
            try
            {
                _analyzer = LuceneIndexingHelpers.CreateLuceneAnalyzer(index, indexDefinition, forQuerying: true);
            }
            catch (Exception e)
            {
                throw new IndexAnalyzerException(e);
            }

            _releaseReadTransaction = directory.SetTransaction(readTransaction, out _state);
            _releaseSearcher = searcherHolder.GetSearcher(readTransaction, _state, out _searcher);
        }

        public override List<FacetResult> FacetedQuery(FacetQuery facetQuery, QueryTimingsScope queryTimings, DocumentsOperationContext context, Func<string, SpatialField> getSpatialField, CancellationToken token)
        {
            var results = FacetedQueryParser.Parse(context, facetQuery, SearchEngineType.Lucene);

            var query = facetQuery.Query;
            Dictionary<string, Dictionary<string, FacetValues>> facetsByName = null;

            var baseQuery = GetLuceneQuery(context, query.Metadata, query.QueryParameters, _analyzer, _queryBuilderFactories);

            List<ReaderFacetInfo> returnedReaders;
            using (queryTimings?.For(nameof(QueryTimingsScope.Names.Lucene)))
                returnedReaders = GetQueryMatchingDocuments(_searcher, baseQuery, _state);

            foreach (var result in results)
            {
                using (var facetTiming = queryTimings?.For($"{nameof(QueryTimingsScope.Names.AggregateBy)}/{result.Key}"))
                {
                    if (result.Value.Ranges == null || result.Value.Ranges.Count == 0)
                    {
                        facetsByName ??= new Dictionary<string, Dictionary<string, FacetValues>>();

                        HandleFacets(returnedReaders, result, facetsByName, facetQuery.Legacy, facetTiming, token);
                        continue;
                    }

                    HandleRangeFacets(returnedReaders, result, facetQuery.Legacy, facetTiming, token);
                }
            }

            UpdateFacetResults(results, query, facetsByName);

            CompleteFacetCalculationsStage(results, query);

            foreach (var readerFacetInfo in returnedReaders)
            {
                IntArraysPool.Instance.FreeArray(readerFacetInfo.Results.Array);
            }

            return results.Values
                .Select(x => x.Result)
                .ToList();
        }

        private void HandleRangeFacets(
            List<ReaderFacetInfo> returnedReaders,
            KeyValuePair<string, FacetedQueryParser.FacetResult> result,
            bool legacy,
            QueryTimingsScope queryTimings,
            CancellationToken token)
        {
            var needToApplyAggregation = result.Value.Aggregations.Count > 0;
            var facetValues = new Dictionary<string, FacetValues>();

            var ranges = result.Value.Ranges;
            foreach (var range in ranges)
            {
                var key = range.RangeText;
                if (facetValues.TryGetValue(key, out var collectionOfFacetValues))
                    continue;

                collectionOfFacetValues = new FacetValues(legacy);
                if (needToApplyAggregation == false)
                    collectionOfFacetValues.AddDefault(key);
                else
                {
                    foreach (var aggregation in result.Value.Aggregations)
                        collectionOfFacetValues.Add(aggregation.Key, key);
                }

                facetValues.Add(key, collectionOfFacetValues);
            }

            foreach (var readerFacetInfo in returnedReaders)
            {
                var name = FieldUtil.ApplyRangeSuffixIfNecessary(result.Value.AggregateBy, result.Value.RangeType);

                Dictionary<string, int[]> termsForField;
                using (queryTimings?.For(nameof(QueryTimingsScope.Names.Terms)))
                    termsForField = IndexedTerms.GetTermsAndDocumentsFor(readerFacetInfo.Reader, readerFacetInfo.DocBase, name, _indexName, _state);

                foreach (var kvp in termsForField)
                {
                    foreach (var range in ranges)
                    {
                        token.ThrowIfCancellationRequested();

                        if (range.IsMatch(kvp.Key) == false)
                            continue;

                        var intersectedDocuments = GetIntersectedDocuments(new ArraySegment<int>(kvp.Value), readerFacetInfo.Results, needToApplyAggregation);
                        var intersectCount = intersectedDocuments.Count;
                        if (intersectCount == 0)
                            continue;

                        var collectionOfFacetValues = facetValues[range.RangeText];
                        collectionOfFacetValues.IncrementCount(intersectCount);

                        if (needToApplyAggregation)
                            ApplyAggregation(result.Value.Aggregations, collectionOfFacetValues, intersectedDocuments.Documents, readerFacetInfo.Reader, readerFacetInfo.DocBase, _state);

                        intersectedDocuments.Free();
                    }
                }
            }

            foreach (var kvp in facetValues)
            {
                if (kvp.Value.Any == false)
                    continue;

                result.Value.Result.Values.AddRange(kvp.Value.GetAll());
            }
        }

        private void HandleFacets(
            List<ReaderFacetInfo> returnedReaders,
            KeyValuePair<string, FacetedQueryParser.FacetResult> result,
            Dictionary<string, Dictionary<string, FacetValues>> facetsByName,
            bool legacy,
            QueryTimingsScope queryTimings,
            CancellationToken token)
        {
            var needToApplyAggregation = result.Value.Aggregations.Count > 0;

            if (facetsByName.TryGetValue(result.Key, out var facetValues) == false)
                facetsByName[result.Key] = facetValues = new Dictionary<string, FacetValues>();

            foreach (var readerFacetInfo in returnedReaders)
            {
                // a reader without matches has nothing to contribute, and loading the field's terms, let alone building the
                // per-document term index, would only fill the caches on its behalf
                if (readerFacetInfo.Results.Count == 0)
                    continue;

                Dictionary<string, int[]> termsForField;
                using (queryTimings?.For(nameof(QueryTimingsScope.Names.Terms)))
                    termsForField = IndexedTerms.GetTermsAndDocumentsFor(readerFacetInfo.Reader, readerFacetInfo.DocBase, result.Value.AggregateBy, _indexName, _state);

                // Intersecting every distinct term with the matches is a full pass over the field's terms on every query,
                // however few documents matched. When the matches are fewer than the terms, the norm for high cardinality
                // fields such as ids, walk the matches and read their terms instead.
                if (readerFacetInfo.Results.Count < termsForField.Count)
                {
                    IndexedTerms.DocumentTermsIndex documentTerms;
                    using (queryTimings?.For(nameof(QueryTimingsScope.Names.Terms)))
                        documentTerms = IndexedTerms.GetDocumentTermsFor(readerFacetInfo.Reader, readerFacetInfo.DocBase, result.Value.AggregateBy, _indexName, _state);

                    if (documentTerms != null)
                    {
                        HandleFacetsByMatchingDocuments(readerFacetInfo, documentTerms, result, facetValues, legacy, needToApplyAggregation, token);
                        continue;
                    }
                }

                foreach (var kvp in termsForField)
                {
                    if (kvp.Value.Length == 0)
                        continue;

                    token.ThrowIfCancellationRequested();

                    var intersectedDocuments = GetIntersectedDocuments(new ArraySegment<int>(kvp.Value), readerFacetInfo.Results, needToApplyAggregation);
                    if (intersectedDocuments.Count == 0)
                        continue;

                    AddTermMatches(readerFacetInfo, result, facetValues, legacy, needToApplyAggregation, kvp.Key, intersectedDocuments.Count, intersectedDocuments.Documents);
                    intersectedDocuments.Free();
                }
            }
        }

        private void HandleFacetsByMatchingDocuments(
            ReaderFacetInfo readerFacetInfo,
            IndexedTerms.DocumentTermsIndex documentTerms,
            KeyValuePair<string, FacetedQueryParser.FacetResult> result,
            Dictionary<string, FacetValues> facetValues,
            bool legacy,
            bool needToApplyAggregation,
            CancellationToken token)
        {
            var matches = readerFacetInfo.Results;
            var array = matches.Array;
            var end = matches.Offset + matches.Count;
            var docBase = readerFacetInfo.DocBase;

            // one pass over the matches counts the matches of every term
            var slicesByTerm = new Dictionary<int, TermSlice>();
            var total = 0;
            for (var i = matches.Offset; i < end; i++)
            {
                token.ThrowIfCancellationRequested();

                foreach (var ordinal in documentTerms.GetTermOrdinals(array[i] - docBase))
                {
                    CollectionsMarshal.GetValueRefOrAddDefault(slicesByTerm, ordinal, out _).Count++;
                    total++;
                }
            }

            if (needToApplyAggregation == false)
            {
                // the counts are all a facet without an aggregation needs
                foreach (var (ordinal, slice) in slicesByTerm)
                    AddTermMatches(readerFacetInfo, result, facetValues, legacy, needToApplyAggregation, documentTerms.Terms[ordinal], slice.Count, ReadOnlySpan<int>.Empty);

                return;
            }

            if (total == 0)
                return;

            // The aggregation reads the matches of every term. A buffer per term would keep one pooled array alive per
            // distinct term until the whole reader is done, hundreds of megabytes when many documents match, so the
            // matches of all terms go next to each other in a single buffer, each term in its own slice: the counts
            // become the slice offsets and a second pass over the matches fills them.
            var offset = 0;
            foreach (var ordinal in slicesByTerm.Keys)
            {
                ref var slice = ref CollectionsMarshal.GetValueRefOrNullRef(slicesByTerm, ordinal);
                slice.Start = offset;
                offset += slice.Count;
            }

            var buffer = IntArraysPool.Instance.AllocateArray(total);
            try
            {
                for (var i = matches.Offset; i < end; i++)
                {
                    token.ThrowIfCancellationRequested();

                    var doc = array[i];
                    foreach (var ordinal in documentTerms.GetTermOrdinals(doc - docBase))
                    {
                        ref var slice = ref CollectionsMarshal.GetValueRefOrNullRef(slicesByTerm, ordinal);
                        buffer[slice.Start + slice.Filled++] = doc;
                    }
                }

                foreach (var (ordinal, slice) in slicesByTerm)
                    AddTermMatches(readerFacetInfo, result, facetValues, legacy, needToApplyAggregation, documentTerms.Terms[ordinal], slice.Count, new ReadOnlySpan<int>(buffer, slice.Start, slice.Count));
            }
            finally
            {
                IntArraysPool.Instance.FreeArray(buffer);
            }
        }

        // the matches of one term within the buffer shared by all the terms of a reader
        private struct TermSlice
        {
            public int Start;
            public int Count;
            public int Filled;
        }

        private void AddTermMatches(
            ReaderFacetInfo readerFacetInfo,
            KeyValuePair<string, FacetedQueryParser.FacetResult> result,
            Dictionary<string, FacetValues> facetValues,
            bool legacy,
            bool needToApplyAggregation,
            string term,
            int count,
            ReadOnlySpan<int> documents)
        {
            if (facetValues.TryGetValue(term, out var collectionOfFacetValues) == false)
            {
                var range = FacetedQueryHelper.GetRangeName(result.Value.AggregateBy, term);
                collectionOfFacetValues = new FacetValues(legacy);
                if (needToApplyAggregation == false)
                    collectionOfFacetValues.AddDefault(range);
                else
                {
                    foreach (var aggregation in result.Value.Aggregations)
                        collectionOfFacetValues.Add(aggregation.Key, range);
                }

                facetValues.Add(term, collectionOfFacetValues);
            }

            collectionOfFacetValues.IncrementCount(count);

            if (needToApplyAggregation)
                ApplyAggregation(result.Value.Aggregations, collectionOfFacetValues, documents, readerFacetInfo.Reader, readerFacetInfo.DocBase, _state);
        }

        private static void ApplyAggregation(Dictionary<FacetAggregationField, FacetedQueryParser.FacetResult.Aggregation> aggregations, FacetValues values, ReadOnlySpan<int> docsInQuery, IndexReader indexReader, int docBase, IState state)
        {
            foreach (var kvp in aggregations)
            {
                if (string.IsNullOrEmpty(kvp.Key.Name)) // Count
                    continue;

                var value = values.Get(kvp.Key);

                var name = FieldUtil.ApplyRangeSuffixIfNecessary(kvp.Key.Name, RangeType.Double);
                var doubles = FieldCache_Fields.DEFAULT.GetDoubles(indexReader, name, state);

                var val = kvp.Value;
                double min = value.Min ?? double.MaxValue, max = value.Max ?? double.MinValue, sum = value.Sum ?? 0, avg = value.Average ?? 0;
                foreach (var doc in docsInQuery)
                {
                    var currentVal = doubles[doc - docBase];
                    sum += currentVal;
                    avg += currentVal;
                    min = Math.Min(min, currentVal);
                    max = Math.Max(max, currentVal);
                }

                if (val.Min)
                {
                    value.Min = min;
                }

                if (val.Average)
                {
                    value.Average = avg;
                }

                if (val.Max)
                {
                    value.Max = max;
                }

                if (val.Sum)
                {
                    value.Sum = sum;
                }
            }
        }

        private static List<ReaderFacetInfo> GetQueryMatchingDocuments(IndexSearcher currentIndexSearcher, Query baseQuery, IState state)
        {
            var gatherAllCollector = new GatherAllCollectorByReader();
            using (Searcher.EnableLightWeightSimilarity())
            {
                currentIndexSearcher.Search(baseQuery, gatherAllCollector, state);
            }


            foreach (var readerFacetInfo in gatherAllCollector.Results)
            {
                readerFacetInfo.Complete();
            }

            return gatherAllCollector.Results;
        }

        /// <summary>
        /// This method expects both lists to be sorted
        /// </summary>
        private IntersectDocs GetIntersectedDocuments(ArraySegment<int> a, ArraySegment<int> b, bool needToApplyAggregation)
        {
            if (a.Count == 0 || b.Count == 0)
                return IntersectDocs.Empty;

            ArraySegment<int> n, m;
            if (a.Count > b.Count)
            {
                n = a;
                m = b;
            }
            else
            {
                n = b;
                m = a;
            }

            int nSize = n.Count;
            int mSize = m.Count;

            int[] nArray = n.Array;
            int[] mArray = m.Array;

            if (n[0] > m[^1] || m[0] > n[^1]) // quick check if intersection is even possible
                return IntersectDocs.Empty;

            double o1 = nSize + mSize;
            double o2 = mSize * Math.Log(nSize, 2);

            var result = new IntersectDocs(needToApplyAggregation);

            if (o1 < o2)
            {
                int mi = m.Offset, ni = n.Offset;
                while (mi < mSize && ni < nSize)
                {
                    var nVal = nArray[ni];
                    var mVal = mArray[mi];

                    if (nVal > mVal)
                    {
                        mi++;
                    }
                    else if (nVal < mVal)
                    {
                        ni++;
                    }
                    else
                    {
                        int docId = nVal;
                        result.AddIntersection(docId);

                        ni++;
                        mi++;
                    }
                }
            }
            else
            {
                for (int i = m.Offset; i < mSize; i++)
                {
                    int docId = mArray[i];
                    if (Array.BinarySearch(nArray, n.Offset, n.Count, docId) >= 0)
                    {
                        result.AddIntersection(docId);
                    }
                }
            }

            return result;
        }

        public override void Dispose()
        {
            _releaseSearcher?.Dispose();
            _releaseReadTransaction?.Dispose();
        }

        private sealed class IntersectDocs
        {
            public static readonly IntersectDocs Empty = new(collectDocuments: false);

            private readonly bool _collectDocuments;

            public int Count;

            // rented from the pool on the first intersection, so the many terms without any cost nothing
            private int[] _documents;

            public IntersectDocs(bool collectDocuments)
            {
                _collectDocuments = collectDocuments;
            }

            // empty unless the documents were collected
            public ReadOnlySpan<int> Documents => _documents == null ? ReadOnlySpan<int>.Empty : new ReadOnlySpan<int>(_documents, 0, Count);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void AddIntersection(int docId)
            {
                if (_collectDocuments)
                {
                    if (_documents == null)
                        _documents = IntArraysPool.Instance.AllocateArray();
                    else if (Count >= _documents.Length)
                        IncreaseSize();

                    _documents[Count] = docId;
                }

                Count++;
            }

            public void Free()
            {
                if (_documents == null)
                    return;

                IntArraysPool.Instance.FreeArray(_documents);
                _documents = null;
            }

            private void IncreaseSize()
            {
                var newDocumentsArray = IntArraysPool.Instance.AllocateArray(Count * 2);
                Array.Copy(_documents, newDocumentsArray, Count);
                IntArraysPool.Instance.FreeArray(_documents);
                _documents = newDocumentsArray;
            }
        }

        private sealed class IntArraysPool : ILowMemoryHandler
        {
            public static readonly IntArraysPool Instance = new IntArraysPool();

            private readonly ConcurrentDictionary<int, ObjectPool<int[]>> _arraysPoolBySize = new ConcurrentDictionary<int, ObjectPool<int[]>>();
            //private readonly TimeSensitiveStore<ObjectPool<int[]>> _timeSensitiveStore = new TimeSensitiveStore<ObjectPool<int[]>>(TimeSpan.FromDays(1));

            private IntArraysPool()
            {
            }

            public int[] AllocateArray(int arraySize = 1024)
            {
                var roundedSize = GetRoundedSize(arraySize);
                var matchingQueue = _arraysPoolBySize.GetOrAdd(roundedSize, x => new ObjectPool<int[]>(() => new int[roundedSize]));

                var allocatedArray = matchingQueue.Allocate();

                //_timeSensitiveStore.Seen(matchingQueue);

                return allocatedArray;
            }

            public void FreeArray(int[] returnedArray)
            {
                if (returnedArray.Length != GetRoundedSize(returnedArray.Length))
                {
                    throw new ArgumentException("Array size does not match current array size constraints");
                }


                var matchingQueue = _arraysPoolBySize.GetOrAdd(returnedArray.Length, x => new ObjectPool<int[]>(() => new int[returnedArray.Length]));
                matchingQueue.Free(returnedArray);
            }

            private static int GetRoundedSize(int size)
            {
                const int roundSize = 1024;
                if (size % roundSize == 0)
                {
                    return size;
                }

                return (size / roundSize + 1) * roundSize;
            }

            private static void RunIdleOperations()
            {
                //_timeSensitiveStore.ForAllExpired(x =>
                //{
                //    var matchingQueue = _arraysPoolBySize.FirstOrDefault(y => y.Value == x).Key;
                //    if (matchingQueue != 0)
                //    {
                //        ObjectPool<int[]> removedQueue;
                //        _arraysPoolBySize.TryRemove(matchingQueue, out removedQueue);
                //    }
                //});
            }

            public void LowMemory(LowMemorySeverity lowMemorySeverity)
            {
                RunIdleOperations();
            }

            public void LowMemoryOver()
            {
            }
        }

        private sealed class ReaderFacetInfo
        {
            public IndexReader Reader;
            public int DocBase;
            // Here we store the _global document id_, if you need the 
            // reader document id, you must decrement with the DocBase
            private readonly LinkedList<int[]> _matches;
            private int[] _current;
            private int _pos;
            public ArraySegment<int> Results;

            public ReaderFacetInfo()
            {
                _current = IntArraysPool.Instance.AllocateArray();
                _matches = new LinkedList<int[]>();
            }

            public void AddMatch(int doc)
            {
                if (_pos >= _current.Length)
                {
                    _matches.AddLast(_current);
                    _current = IntArraysPool.Instance.AllocateArray();
                    _pos = 0;
                }
                _current[_pos++] = doc + DocBase;
            }

            public void Complete()
            {
                var size = _pos;
                foreach (var match in _matches)
                {
                    size += match.Length;
                }

                var mergedAndSortedArray = IntArraysPool.Instance.AllocateArray(size);
                var curMergedArrayIndex = 0;
                foreach (var match in _matches)
                {
                    Array.Copy(match, 0, mergedAndSortedArray, curMergedArrayIndex, match.Length);
                    curMergedArrayIndex += match.Length;
                    IntArraysPool.Instance.FreeArray(match);
                }

                Array.Copy(_current, 0, mergedAndSortedArray, curMergedArrayIndex, _pos);
                IntArraysPool.Instance.FreeArray(_current);
                curMergedArrayIndex += _pos;
                _current = null;
                _pos = 0;

                Array.Sort(mergedAndSortedArray, 0, curMergedArrayIndex);
                Results = new ArraySegment<int>(mergedAndSortedArray, 0, curMergedArrayIndex);
            }
        }

        private sealed class GatherAllCollectorByReader : Collector
        {
            private ReaderFacetInfo _current;
            public readonly List<ReaderFacetInfo> Results = new List<ReaderFacetInfo>();

            public override void SetScorer(Scorer scorer)
            {
            }

            public override void Collect(int doc, IState state)
            {
                _current.AddMatch(doc);
            }

            public override void SetNextReader(IndexReader reader, int docBase, IState state)
            {
                _current = new ReaderFacetInfo
                {
                    DocBase = docBase,
                    Reader = reader
                };
                Results.Add(_current);
            }

            public override bool AcceptsDocsOutOfOrder => true;
        }


    }
}
