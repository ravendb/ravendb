using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Windows.Markup;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Raven.Abstractions;
using Lucene.Net.Search;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Database.Config;
using Voron.Util;
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
            if (searcher != null && 
				context.Configuration.PrewarmFacetsOnIndexingMaxAge != TimeSpan.Zero)
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


        internal class IndexSearcherHoldingState : IDisposable, ILowMemoryHandler
        {
            public readonly IndexSearcher IndexSearcher;

            public volatile bool ShouldDispose;
            public int Usage;
            private RavenJObject[] readEntriesFromIndex;
            private readonly Lazy<ManualResetEvent> disposed = new Lazy<ManualResetEvent>(() => new ManualResetEvent(false));

            private readonly ConcurrentDictionary<string, DateTime> lastFacetQuery = new ConcurrentDictionary<string, DateTime>();

            private readonly ConcurrentDictionary<Tuple<int, uint>, StringCollectionValue> docsCache = new ConcurrentDictionary<Tuple<int, uint>, StringCollectionValue>();

            private readonly ReaderWriterLockSlim rwls = new ReaderWriterLockSlim();
			private readonly Dictionary<string, Dictionary<string, HashSet<int>>> cache = new Dictionary<string, Dictionary<string, HashSet<int>>>();

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

			public Dictionary<string, HashSet<int>> GetFromCache(string field)
            {
				Dictionary<string, HashSet<int>> vals;
				cache.TryGetValue(field, out vals);
				return vals;
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

				MemoryStatistics.RegisterLowMemoryHandler(this);
            }

	        public void HandleLowMemory()
	        {
				rwls.EnterWriteLock();
		        try
		        {
					lastFacetQuery.Clear();
					cache.Clear();
                    docsCache.Clear();
		        }
		        finally
		        {
					rwls.ExitWriteLock();
		        }
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

			[MethodImpl(MethodImplOptions.AggressiveInlining)]
	        public void SetInCache(string field, Dictionary<string, HashSet<int>> docsPerTerm)
	        {
				cache[field] = docsPerTerm;
	        }

            public StringCollectionValue GetFieldsValues(int docId, uint fieldsCrc, string[] fields)
            {
                var key = Tuple.Create(docId, fieldsCrc);

                StringCollectionValue value;
                if (docsCache.TryGetValue(key, out value))
                    return value;

                return docsCache.GetOrAdd(key, _ =>
                {
                    var doc = IndexSearcher.Doc(docId);
	                return new StringCollectionValue((from field in fields
		                from fld in doc.GetFields(field)
		                where fld.StringValue != null
		                select fld.StringValue).ToList());
                });
                
            }
        }

        public class StringCollectionValue
        {
            private readonly int _hashCode;
            private uint _crc;
#if DEBUG
// ReSharper disable once NotAccessedField.Local
	        private List<string> _values;
#endif

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                var other = obj as StringCollectionValue;
                if (other == null) return false;

                return _crc == other._crc;
            }

            public override int GetHashCode()
            {
                return _hashCode;
            }

            public StringCollectionValue(List<string> values)
            {
#if DEBUG
	            _values = values;
#endif
                if (values.Count == 0)
                    throw new InvalidOperationException("Cannot apply distinct facet on empty fields, did you forget to store them in the index? ");

                _hashCode = values.Count;
                _crc = (uint)values.Count;
                foreach (string s in values)
                {
                    unchecked
                    {
                        _hashCode = _hashCode * 397 ^ s.GetHashCode();
                    }
                    var curValue = s;
                    _crc = Crc.Value(curValue, _crc);
                }
            }
        }

	    internal class GatherAllDiscintCollector : Collector
		{
			private readonly IndexQuery _query;
			private readonly IndexSearcherHoldingState _state;
			private int _docBase;
			private readonly HashSet<int> _documents = new HashSet<int>();
			private readonly HashSet<StringCollectionValue> _alreadySeen = new HashSet<StringCollectionValue>();
		    private uint _fieldsCrc;

		    public GatherAllDiscintCollector(IndexQuery query, IndexSearcherHoldingState state)
			{
				if (query.IsDistinct == false)
					throw new ArgumentException("Only distinct queries allowed");
				_query = query;
				_state = state;
				_fieldsCrc = query.FieldsToFetch.Aggregate<string, uint>(0, (current, field) => Crc.Value(field, current));
			}

			public override void SetScorer(Scorer scorer)
			{
			}

			public override void Collect(int doc)
			{
				var fields = _state.GetFieldsValues(doc, _fieldsCrc, _query.FieldsToFetch);

				if (_alreadySeen.Add(fields) == false)
					return;

				_documents.Add(doc + _docBase);
			}

			public override void SetNextReader(IndexReader reader, int docBase)
			{
				_docBase = docBase;
			}

			public override bool AcceptsDocsOutOfOrder
			{
				get { return true; }
			}

			public HashSet<int> Documents
			{
				get { return _documents; }
			}
		}
    }
}
