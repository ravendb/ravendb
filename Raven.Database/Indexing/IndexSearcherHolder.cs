using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using Lucene.Net.Index;
using Raven.Abstractions;
using Lucene.Net.Search;
using Task = System.Threading.Tasks.Task;
using Raven.Abstractions.Logging;
using Raven.Json.Linq;

namespace Raven.Database.Indexing
{
    public class IndexSearcherHolder
    {
        private readonly int indexId;
        private readonly WorkContext context;
        private static readonly ILog Log = LogManager.GetCurrentClassLogger();

        private volatile IndexSearcherHoldingState current;

        public IndexSearcherHolder(int indexId, WorkContext context)
        {
            this.indexId = indexId;
            this.context = context;
        }

        public ManualResetEvent SetIndexSearcher(IndexSearcher searcher, bool wait)
        {
            var old = current;
            current = new IndexSearcherHoldingState(searcher);

            if (old == null)
                return null;

            // here we try to make sure that the actual facet cache is up to do when we update the index searcher.
            // we use this to ensure that any facets that has been recently queried is warmed up and in the cache
            if (context.Configuration.PrewarmFacetsOnIndexingMaxAge != TimeSpan.Zero)
            {
                var usedFacets = old.GetUsedFacets(context.Configuration.PrewarmFacetsOnIndexingMaxAge).ToArray();

                if (usedFacets.Length > 0)
                {
                    var preFillCache = Task.Factory.StartNew(() =>
                    {
                        var sp = Stopwatch.StartNew();
                        try
                        {
                            IndexedTerms.PreFillCache(current, usedFacets, searcher.IndexReader);
                        }
                        catch (Exception e)
                        {
                            Log.WarnException(
                                string.Format("Failed to properly pre-warm the facets cache ({1}) for index {0}", indexId,
                                    string.Join(",", usedFacets)), e);
                        }
                        finally
                        {
                            Log.Debug("Pre-warming the facet cache for {0} took {2}. Facets: {1}", indexId, string.Join(",", usedFacets), sp.Elapsed);
                        }
                    });
                    preFillCache.Wait(context.Configuration.PrewarmFacetsSyncronousWaitTime);
                }
            }


            Interlocked.Increment(ref old.Usage);
            using (old)
            {
                if (wait)
                    return old.MarkForDisposalWithWait();
                old.MarkForDisposal();
                return null;
            }
        }

        public IDisposable GetSearcher(out IndexSearcher searcher)
        {
            var indexSearcherHoldingState = GetCurrentStateHolder();
            try
            {
                searcher = indexSearcherHoldingState.IndexSearcher;
                return indexSearcherHoldingState;
            }
            catch (Exception e)
            {
                Log.ErrorException("Failed to get the index searcher.", e);
                indexSearcherHoldingState.Dispose();
                throw;
            }
        }

        public IDisposable GetSearcherAndTermDocs(out IndexSearcher searcher, out RavenJObject[] termDocs)
        {
            var indexSearcherHoldingState = GetCurrentStateHolder();
            try
            {
                searcher = indexSearcherHoldingState.IndexSearcher;
                termDocs = indexSearcherHoldingState.GetOrCreateTerms();
                return indexSearcherHoldingState;
            }
            catch (Exception)
            {
                indexSearcherHoldingState.Dispose();
                throw;
            }
        }

        internal IndexSearcherHoldingState GetCurrentStateHolder()
        {
            while (true)
            {
                var state = current;
                Interlocked.Increment(ref state.Usage);
                if (state.ShouldDispose)
                {
                    state.Dispose();
                    continue;
                }

                return state;
            }
        }


        public class IndexSearcherHoldingState : IDisposable
        {
            public readonly IndexSearcher IndexSearcher;

            public volatile bool ShouldDispose;
            public int Usage;
            private RavenJObject[] readEntriesFromIndex;
            private readonly Lazy<ManualResetEvent> disposed = new Lazy<ManualResetEvent>(() => new ManualResetEvent(false));

            private readonly ConcurrentDictionary<string, DateTime> lastFacetQuery = new ConcurrentDictionary<string, DateTime>();

            private readonly ReaderWriterLockSlim rwls = new ReaderWriterLockSlim();
	        private readonly Dictionary<string, LinkedList<CacheVal>[]> cache = new Dictionary<string, LinkedList<CacheVal>[]>();

	        public ReaderWriterLockSlim Lock
            {
                get { return rwls; }
            }

            public class CacheVal
            {
                public Term Term;
                public double? Val;

	            public override string ToString()
	            {
		            return string.Format("Term: {0}, Val: {1}", Term, Val);
	            }
            }

            public IEnumerable<CacheVal> GetFromCache(string field, int doc)
            {
	            LinkedList<CacheVal>[] vals;
	            if (cache.TryGetValue(field, out vals) == false)
                    yield break;
	            if (vals[doc] == null)
                    yield break;

	            foreach (var cacheVal in vals[doc])
                {
		            yield return cacheVal;
                }
            }

	        public IEnumerable<Term> GetTermsFromCache(string field, int doc)
            {
                return GetFromCache(field, doc).Select(cacheVal => cacheVal.Term);
            }

            public IEnumerable<string> GetUsedFacets(TimeSpan tooOld)
            {
                var now = SystemTime.UtcNow;
                return lastFacetQuery.Where(x => (now - x.Value) < tooOld).Select(x => x.Key);
            }

            public bool IsInCache(string field)
            {
                var now = SystemTime.UtcNow;
                lastFacetQuery.AddOrUpdate(field, now, (s, time) => time > now ? time : now);
                return cache.ContainsKey(field);
            }

            public IndexSearcherHoldingState(IndexSearcher indexSearcher)
            {
                IndexSearcher = indexSearcher;
            }

            public void MarkForDisposal()
            {
                ShouldDispose = true;
            }

            public ManualResetEvent MarkForDisposalWithWait()
            {
                var x = disposed.Value;//  first create the value
                ShouldDispose = true;
                return x;
            }

            public void Dispose()
            {
                if (Interlocked.Decrement(ref Usage) > 0)
                    return;
                if (ShouldDispose == false)
                    return;
                DisposeRudely();
            }

            private void DisposeRudely()
            {
                if (IndexSearcher != null)
                {
                    using (IndexSearcher)
                    using (IndexSearcher.IndexReader) { }
                }
                if (disposed.IsValueCreated)
                    disposed.Value.Set();
            }


            [MethodImpl(MethodImplOptions.Synchronized)]
            public RavenJObject[] GetOrCreateTerms()
            {
                if (readEntriesFromIndex != null)
                    return readEntriesFromIndex;

                var indexReader = IndexSearcher.IndexReader;
                readEntriesFromIndex = IndexedTerms.ReadAllEntriesFromIndex(indexReader);
                return readEntriesFromIndex;
            }

	        public void SetInCache(string field, LinkedList<CacheVal>[] items)
	        {
		        cache[field] = items;
	        }
        }
    }
}
