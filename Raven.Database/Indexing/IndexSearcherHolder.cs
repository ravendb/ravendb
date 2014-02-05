using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Raven.Abstractions;
using Raven.Abstractions.Logging;
using Raven.Database.Tasks;
using Raven.Json.Linq;
using Raven.Abstractions.Extensions;
using Task = System.Threading.Tasks.Task;

namespace Raven.Database.Indexing
{
    public class IndexSearcherHolder
    {
        private readonly WorkContext context;
        private static readonly ILog Log = LogManager.GetCurrentClassLogger();

        private volatile IndexSearcherHoldingState current;

        public IndexSearcherHolder(WorkContext context)
        {
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
                        IndexedTerms.PreFillCache(current, usedFacets, searcher.IndexReader)
                        );
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


            private readonly Dictionary<string, Dictionary<int, List<CacheVal>>> cache =
                new Dictionary<string, Dictionary<int, List<CacheVal>>>();

            private readonly ConcurrentDictionary<string, DateTime> lastFacetQuery = new ConcurrentDictionary<string, DateTime>();

            private readonly ReaderWriterLockSlim rwls = new ReaderWriterLockSlim();

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
                Dictionary<int, List<CacheVal>> value;
                if (cache.TryGetValue(field, out value) == false)
                    yield break;
                List<CacheVal> list;
                if (value.TryGetValue(doc, out list) == false)
                    yield break;
                foreach (var item in list)
                {
                    yield return item;
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
            public void SetInCache(string field, int doc, Term term, double? val = null)
            {
                cache.GetOrAdd(field).GetOrAdd(doc).Add(new CacheVal
                {
                    Term = term,
                    Val = val
                });
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
        }
    }
}
