using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Sparrow.LowMemory;

namespace Raven.Server.Documents.Queries.LuceneIntegration
{
    public class CachingQuery : Query
    {
        private static readonly ConditionalWeakTable<object, ConcurrentDictionary<string, FastBitArray>> _readerCache = new();

        private readonly Query _inner;

        public CachingQuery(Query inner)
        {
            _inner = inner;
        }

        public override Query Rewrite(IndexReader reader, IState state)
        {
            Query rewrite = _inner.Rewrite(reader, state);
            if (ReferenceEquals(rewrite, _inner))
                return this;

            return new CachingQuery(rewrite);
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
                var cacheKey = _parent.ToString();

                var cache = _readerCache.GetOrCreateValue(reader);
                Debug.Assert(cache != null, nameof(cache) + " != null");
                // This is immutable, so we can safely cache the values for this segment
                if (cache.TryGetValue(cacheKey, out var results) == false)
                {
                    var scorer = _inner.Scorer(reader, scoreDocsInOrder, topScorer, state);
                    results = new FastBitArray(reader.MaxDoc);
                    while (true)
                    {
                        int doc = scorer.NextDoc(state);
                        if (doc == DocIdSetIterator.NO_MORE_DOCS)
                            break;
                        results.Set(doc);
                    }
                    cache.TryAdd(cacheKey, results);
                }

                Similarity similarity = _parent.GetSimilarity(_searcher);
                return new FastBitArrayScorer(results, similarity);
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
            return $"Caching({_inner.ToString(field)})";
        }
    }
}
