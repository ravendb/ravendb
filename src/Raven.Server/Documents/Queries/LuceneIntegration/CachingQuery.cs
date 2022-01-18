using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using JetBrains.Annotations;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Microsoft.Extensions.Caching.Memory;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Collections;
using Sparrow.LowMemory;
using Sparrow.Threading;
using Index = Raven.Server.Documents.Indexes.Index;
using MemoryCache = Raven.Server.Utils.Imports.Memory.MemoryCache;

namespace Raven.Server.Documents.Queries.LuceneIntegration
{
    public class CachingQuery : Query
    {
        private static ConditionalWeakTable<IndexReader, IndexReaderCachedQueries> CacheByReader = new();

        public static MultipleUseFlag InLowMemoryMode = new();
        
        [UsedImplicitly]
        private static CacheByReaderCleaner Cleaner = new();

        private class CacheByReaderCleaner : ILowMemoryHandler
        {
            public CacheByReaderCleaner()
            {
                LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);
            }
            
            public void LowMemory(LowMemorySeverity lowMemorySeverity)
            {
                InLowMemoryMode.Raise();
                if (lowMemorySeverity != LowMemorySeverity.ExtremelyLow)
                    return;
                // can't call clear, etc, just forget the whole thing, so GC will clean
                CacheByReader = new();
            }

            public void LowMemoryOver()
            {
                InLowMemoryMode.Lower();
            }
        }

        public class QueryCacheKey
        {
            public string Query;
            public string Owner;
            public string Database;
            public string Index;

            protected bool Equals(QueryCacheKey other)
            {
                return Query == other.Query && Owner == other.Owner;
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != this.GetType()) return false;
                return Equals((QueryCacheKey)obj);
            }

            public override int GetHashCode()
            {
                return HashCode.Combine(Query, Owner);
            }
        }
        
        public class IndexReaderCachedQueries
        {
            public SizeLimitedConcurrentDictionary<QueryCacheKey, DateTime> PreviousClauses;
            public ConcurrentSet<string> CachedQueries = new();
            public MemoryCache Cache;
            // This is globally unique, but this value is per reader, so we 
            // can control over memory better at the database level
            public string UniqueId;
            public WeakReference WeakSelf;

            public IndexReaderCachedQueries(Index index)
            {
                UniqueId = Guid.NewGuid().ToString();
                PreviousClauses = new(index.Configuration.QueryClauseCacheRepeatedQueriesCount);
                WeakSelf = new(this);
            }

            ~IndexReaderCachedQueries()
            {
                var key = new QueryCacheKey
                {
                    Owner = UniqueId
                };
                foreach (string cachedQuery in CachedQueries)
                {
                    key.Query = cachedQuery;
                    try
                    {
                        Cache.Remove(key);
                    }
                    catch (ObjectDisposedException)
                    {
                        // we may run this after the cache is released, so 
                        // might nothing needs to be done here
                        return;
                    }
                }
            }
        }

        private readonly Query _inner;
        private readonly Raven.Server.Documents.Indexes.Index _index;
        private readonly string _query;

        public CachingQuery(Query inner, Raven.Server.Documents.Indexes.Index index, string query)
        {
            _inner = inner;
            _index = index;
            _query = query;
        }

        public override Query Rewrite(IndexReader reader, IState state)
        {
            Query rewrite = _inner.Rewrite(reader, state);
            if (ReferenceEquals(rewrite, _inner))
                return this;

            return new CachingQuery(rewrite, _index, _query);
        }

        public override Weight CreateWeight(Searcher searcher, IState state)
        {
            return new CachingWeight(this, _inner.CreateWeight(searcher, state), searcher);
        }

        private class CachingWeight : Weight
        {
            private readonly CachingQuery _parent;
            private readonly Weight _inner;
            private readonly Searcher _searcher;

            public CachingWeight(CachingQuery parent, Weight inner, Searcher searcher)
            {
                _parent = parent;
                _inner = inner;
                _searcher = searcher;
            }

            public override Lucene.Net.Search.Explanation Explain(IndexReader reader, int doc, IState state)
            {
                return _inner.Explain(reader, doc, state);
            }

            public override void Normalize(float norm)
            {
                _inner.Normalize(norm);
            }

            public override Scorer Scorer(IndexReader reader, bool scoreDocsInOrder, bool topScorer, IState state)
            {
                Debug.Assert(reader is ReadOnlySegmentReader); // we assume that only segments go here

                var clauseCache = _parent._index.DocumentDatabase.ServerStore.QueryClauseCache;
                if (CacheByReader.TryGetValue(reader, out IndexReaderCachedQueries cachedQueries) == false)
                {
                    cachedQueries = CacheByReader.GetValue(reader, _ => new IndexReaderCachedQueries(_parent._index)
                    {
                        Cache = clauseCache,
                    });
                }
                
                // The reader is immutable, so we can safely cache the values for this segment
                Debug.Assert(cachedQueries != null, nameof(cachedQueries) + " != null");
                var queryCacheKey = new QueryCacheKey { Owner = cachedQueries.UniqueId, Query = _parent._query, };
                DateTime now = DateTime.UtcNow;
                FastBitArray results;
                if (clauseCache.TryGetValue(queryCacheKey, out ulong[] buffer) == false)
                {
                    var scorer = _inner.Scorer(reader, scoreDocsInOrder, topScorer, state);
                    // we only add the clause to the cache after if we see it more than once in a 5 minutes period
                    if (cachedQueries.PreviousClauses.TryGetValue(queryCacheKey, out var previouslySeen) == false ||
                        (now - previouslySeen) > _parent._index.Configuration.QueryClauseCacheRepeatedQueriesTimeFrame.AsTimeSpan)
                    {
                        cachedQueries.PreviousClauses.Set(queryCacheKey, now);
                        return scorer;
                    }

                    if (InLowMemoryMode.IsRaised())
                        return scorer;// let's not do any caching in this mode... 
                        
                    results = new FastBitArray(reader.MaxDoc);
                    while (true)
                    {
                        int doc = scorer.NextDoc(state);
                        if (doc == DocIdSetIterator.NO_MORE_DOCS)
                            break;
                        results.Set(doc);
                    }


                    queryCacheKey.Database = _parent._index.DocumentDatabase.Name;
                    queryCacheKey.Index = _parent._index.Name;
                    
                    clauseCache.Set(queryCacheKey, 
                        results.Bits,
                        new MemoryCacheEntryOptions
                        {
                            Size = results.Size.GetValue(SizeUnit.Bytes), 
                            PostEvictionCallbacks =
                            {
                                new PostEvictionCallbackRegistration
                                {
                                    State = cachedQueries.WeakSelf,
                                    EvictionCallback = EvictionCallback
                                }
                            }
                        });
                    cachedQueries.CachedQueries.Add(queryCacheKey.Query);
                }
                else
                {
                    results = new FastBitArray(buffer);
                }

                Similarity similarity = _parent.GetSimilarity(_searcher);
                return new FastBitArrayScorer(results, similarity, disposeArray: false);
            }

            private class ReturnBuffer
            {
                public ulong[] Buffer;

                ~ReturnBuffer()
                {
                    if (Buffer == null) 
                        return;
                    ArrayPool<ulong>.Shared.Return(Buffer);
                }
            }

            // https://ayende.com/blog/195203-A/challenge-the-code-review-bug-that-gives-me-nightmares-the-fix
            private static ConditionalWeakTable<object, object> _joinLifetimes = new();

            
            private static void EvictionCallback(object key, object value, EvictionReason _, object state)
            {
                if (((WeakReference)state).Target is IndexReaderCachedQueries ircq)
                {
                    var ck = (QueryCacheKey)key;
                    ircq.CachedQueries.TryRemove(ck.Query);
                }

                var array = (ulong[])value;
                
                _joinLifetimes.Add(array, new ReturnBuffer{Buffer = array});
            }

            public override float GetSumOfSquaredWeights()
            {
                return _inner.GetSumOfSquaredWeights();
            }

            public override Query Query => _parent;
            public override float Value => _inner.Value;
        }


        protected bool Equals(CachingQuery other)
        {
            return base.Equals(other) && Equals(_inner, other._inner);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((CachingQuery)obj);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(base.GetHashCode(), _inner);
        }

        public override string ToString(string field)
        {
            return $"Caching({_query})";
        }
    }
}
