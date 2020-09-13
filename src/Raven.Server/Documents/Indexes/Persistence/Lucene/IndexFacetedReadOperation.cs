using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
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
using Raven.Server.Exceptions;
using Raven.Server.Indexing;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public class IndexFacetedReadOperation : IndexOperationBase
    {
        private readonly QueryBuilderFactories _queryBuilderFactories;
        private readonly IndexSearcher _searcher;
        private readonly IDisposable _releaseReadTransaction;
        private readonly RavenPerFieldAnalyzerWrapper _analyzer;
        private readonly IndexSearcherHolder.IndexSearcherHoldingState _currentStateHolder;

        private readonly IState _state;

        public IndexFacetedReadOperation(Index index,
            IndexDefinitionBase indexDefinition,
            LuceneVoronDirectory directory,
            IndexSearcherHolder searcherHolder,
            QueryBuilderFactories queryBuilderFactories,
            Transaction readTransaction,
            DocumentDatabase documentDatabase)
            : base(index, LoggingSource.Instance.GetLogger<IndexFacetedReadOperation>(documentDatabase.Name))
        {
            try
            {
                _analyzer = CreateAnalyzer(() => new LowerCaseKeywordAnalyzer(), indexDefinition, forQuerying: true);
            }
            catch (Exception e)
            {
                throw new IndexAnalyzerException(e);
            }

            _queryBuilderFactories = queryBuilderFactories;
            _releaseReadTransaction = directory.SetTransaction(readTransaction, out _state);
            _currentStateHolder = searcherHolder.GetStateHolder(readTransaction);
            _searcher = _currentStateHolder.GetIndexSearcher(_state);
        }

        public List<FacetResult> FacetedQuery(FacetQuery facetQuery, DocumentsOperationContext context, Func<string, SpatialField> getSpatialField, CancellationToken token)
        {
            var results = FacetedQueryParser.Parse(context, facetQuery);

            var query = facetQuery.Query;
            var facetsByName = new Dictionary<string, Dictionary<string, FacetValue>>();

            var baseQuery = GetLuceneQuery(context, query.Metadata, query.QueryParameters, _analyzer, _queryBuilderFactories);
            var returnedReaders = GetQueryMatchingDocuments(_searcher, baseQuery, _state);

            foreach (var result in results)
            {
                if (result.Value.Ranges == null || result.Value.Ranges.Count == 0)
                {
                    HandleFacets(returnedReaders, result, facetsByName, token);
                    continue;
                }

                HandleRangeFacets(result, returnedReaders, token);
            }

            UpdateFacetResults(results, query, facetsByName);

            CompleteFacetCalculationsStage(results);

            foreach (var readerFacetInfo in returnedReaders)
            {
                IntArraysPool.Instance.FreeArray(readerFacetInfo.Results.Array);
            }

            return results.Values
                .Select(x => x.Result)
                .ToList();
        }

        private void HandleRangeFacets(KeyValuePair<string, FacetedQueryParser.FacetResult> result, List<ReaderFacetInfo> returnedReaders, CancellationToken token)
        {
            var needToApplyAggregation = result.Value.Aggregations.Count > 0;

            foreach (var readerFacetInfo in returnedReaders)
            {
                var name = FieldUtil.ApplyRangeSuffixIfNecessary(result.Value.AggregateBy, result.Value.RangeType);
                var termsForField = IndexedTerms.GetTermsAndDocumentsFor(readerFacetInfo.Reader, readerFacetInfo.DocBase, name, _indexName, _state);

                var ranges = result.Value.Ranges;
                foreach (var kvp in termsForField)
                {
                    token.ThrowIfCancellationRequested();

                    for (int i = 0; i < ranges.Count; i++)
                    {
                        var parsedRange = ranges[i];
                        if (parsedRange.IsMatch(kvp.Key))
                        {
                            var facetValue = result.Value.Result.Values[i];

                            var intersectedDocuments = GetIntersectedDocuments(new ArraySegment<int>(kvp.Value), readerFacetInfo.Results, needToApplyAggregation);
                            var intersectCount = intersectedDocuments.Count;
                            if (intersectCount == 0)
                                continue;

                            facetValue.Count += intersectCount;

                            if (needToApplyAggregation)
                            {
                                var docsInQuery = new ArraySegment<int>(intersectedDocuments.Documents, 0, intersectedDocuments.Count);
                                ApplyAggregation(result.Value.Aggregations, facetValue, docsInQuery, readerFacetInfo.Reader, readerFacetInfo.DocBase, _state);
                                IntArraysPool.Instance.FreeArray(intersectedDocuments.Documents);
                                intersectedDocuments.Documents = null;
                            }
                        }
                    }
                }
            }
        }

        private void HandleFacets(List<ReaderFacetInfo> returnedReaders, KeyValuePair<string, FacetedQueryParser.FacetResult> result, Dictionary<string, Dictionary<string, FacetValue>> facetsByName, CancellationToken token)
        {
            foreach (var readerFacetInfo in returnedReaders)
            {
                var termsForField = IndexedTerms.GetTermsAndDocumentsFor(readerFacetInfo.Reader, readerFacetInfo.DocBase, result.Value.AggregateBy, _indexName, _state);

                if (facetsByName.TryGetValue(result.Key, out Dictionary<string, FacetValue> facetValues) == false)
                    facetsByName[result.Key] = facetValues = new Dictionary<string, FacetValue>();

                foreach (var kvp in termsForField)
                {
                    token.ThrowIfCancellationRequested();

                    var needToApplyAggregation = result.Value.Aggregations.Count > 0;
                    var intersectedDocuments = GetIntersectedDocuments(new ArraySegment<int>(kvp.Value), readerFacetInfo.Results, needToApplyAggregation);
                    var intersectCount = intersectedDocuments.Count;
                    if (intersectCount == 0)
                        continue;

                    if (facetValues.TryGetValue(kvp.Key, out FacetValue facetValue) == false)
                    {
                        facetValue = new FacetValue
                        {
                            Range = FacetedQueryHelper.GetRangeName(result.Value.AggregateBy, kvp.Key)
                        };
                        facetValues.Add(kvp.Key, facetValue);
                    }

                    facetValue.Count += intersectCount;

                    if (needToApplyAggregation)
                    {
                        var docsInQuery = new ArraySegment<int>(intersectedDocuments.Documents, 0, intersectedDocuments.Count);
                        ApplyAggregation(result.Value.Aggregations, facetValue, docsInQuery, readerFacetInfo.Reader, readerFacetInfo.DocBase, _state);
                    }
                }
            }
        }

        private static void UpdateFacetResults(Dictionary<string, FacetedQueryParser.FacetResult> results, IndexQueryServerSide query, Dictionary<string, Dictionary<string, FacetValue>> facetsByName)
        {
            foreach (var result in results)
            {
                if (result.Value.Ranges != null && result.Value.Ranges.Count > 0)
                    continue;

                var values = new List<FacetValue>();
                List<string> allTerms;
                if (facetsByName.TryGetValue(result.Key, out Dictionary<string, FacetValue> groups) == false || groups == null)
                    continue;

                switch (result.Value.Options.TermSortMode)
                {
                    case FacetTermSortMode.ValueAsc:
                        allTerms = new List<string>(groups.OrderBy(x => x.Key).ThenBy(x => x.Value.Count).Select(x => x.Key));
                        break;
                    case FacetTermSortMode.ValueDesc:
                        allTerms = new List<string>(groups.OrderByDescending(x => x.Key).ThenBy(x => x.Value.Count).Select(x => x.Key));
                        break;
                    case FacetTermSortMode.CountAsc:
                        allTerms = new List<string>(groups.OrderBy(x => x.Value.Count).ThenBy(x => x.Key).Select(x => x.Key));
                        break;
                    case FacetTermSortMode.CountDesc:
                        allTerms = new List<string>(groups.OrderByDescending(x => x.Value.Count).ThenBy(x => x.Key).Select(x => x.Key));
                        break;
                    default:
                        throw new ArgumentException(string.Format("Could not understand '{0}'", result.Value.Options.TermSortMode));
                }

                var start = result.Value.Options.Start;
                var pageSize = Math.Min(allTerms.Count, result.Value.Options.PageSize);

                foreach (var term in allTerms.Skip(start).TakeWhile(term => values.Count < pageSize))
                {
                    if (groups.TryGetValue(term, out FacetValue facetValue) == false || facetValue == null)
                        facetValue = new FacetValue { Range = term };

                    values.Add(facetValue);
                }

                var previousHits = allTerms.Take(start).Sum(allTerm =>
                {
                    if (groups.TryGetValue(allTerm, out FacetValue facetValue) == false || facetValue == null)
                        return 0;

                    return facetValue.Count;
                });

                result.Value.Result = new FacetResult
                {
                    Name = result.Key,
                    Values = values,
                    RemainingTermsCount = allTerms.Count - (start + values.Count),
                    RemainingHits = groups.Values.Sum(x => x.Count) - (previousHits + values.Sum(x => x.Count))
                };

                if (result.Value.Options.IncludeRemainingTerms)
                    result.Value.Result.RemainingTerms = allTerms.Skip(start + values.Count).ToList();
            }
        }

        private static void CompleteFacetCalculationsStage(Dictionary<string, FacetedQueryParser.FacetResult> results)
        {
            foreach (var result in results)
            {
                foreach (var value in result.Value.Result.Values)
                {
                    if (value.Average.HasValue == false)
                        continue;

                    if (value.Count == 0)
                        value.Average = double.NaN;
                    else
                        value.Average = value.Average / value.Count;
                }
            }
        }

        private static void ApplyAggregation(Dictionary<string, FacetedQueryParser.FacetResult.Aggregation> aggregations, FacetValue value, ArraySegment<int> docsInQuery, IndexReader indexReader, int docBase, IState state)
        {
            foreach (var kvp in aggregations)
            {
                if (kvp.Key == null) // Count
                    continue;

                var name = FieldUtil.ApplyRangeSuffixIfNecessary(kvp.Key, RangeType.Double);
                var doubles = FieldCache_Fields.DEFAULT.GetDoubles(indexReader, name, state);

                for (var index = 0; index < docsInQuery.Count; index++)
                {
                    var doc = docsInQuery.Array[index];
                    var currentVal = doubles[doc - docBase];

                    if (kvp.Value.Average)
                        value.Average = currentVal + (value.Average ?? 0d);

                    if (kvp.Value.Min)
                        value.Min = Math.Min(value.Min ?? double.MaxValue, currentVal);

                    if (kvp.Value.Max)
                        value.Max = Math.Max(value.Max ?? double.MinValue, currentVal);

                    if (kvp.Value.Sum)
                        value.Sum = currentVal + (value.Sum ?? 0d);
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

            double o1 = nSize + mSize;
            double o2 = mSize * Math.Log(nSize, 2);

            var result = new IntersectDocs();
            if (needToApplyAggregation)
            {
                result.Documents = IntArraysPool.Instance.AllocateArray();
            }

            if (o1 < o2)
            {
                int mi = m.Offset, ni = n.Offset;
                while (mi < mSize && ni < nSize)
                {
                    var nVal = n.Array[ni];
                    var mVal = m.Array[mi];

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
                    int docId = m.Array[i];
                    if (Array.BinarySearch(n.Array, n.Offset, n.Count, docId) >= 0)
                    {
                        result.AddIntersection(docId);
                    }
                }
            }
            return result;
        }

        public override void Dispose()
        {
            _currentStateHolder?.Dispose();
            _releaseReadTransaction?.Dispose();
        }

        private class IntersectDocs
        {
            public int Count;
            public int[] Documents;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void AddIntersection(int docId)
            {
                if (Documents != null)
                {
                    if (Count >= Documents.Length)
                    {
                        IncreaseSize();
                    }
                    Documents[Count] = docId;
                }
                Count++;
            }

            private void IncreaseSize()
            {
                var newDocumentsArray = IntArraysPool.Instance.AllocateArray(Count * 2);
                Array.Copy(Documents, newDocumentsArray, Count);
                IntArraysPool.Instance.FreeArray(Documents);
                Documents = newDocumentsArray;
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

        private class ReaderFacetInfo
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

        private class GatherAllCollectorByReader : Collector
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
