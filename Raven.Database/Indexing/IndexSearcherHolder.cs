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

        public ManualResetEvent SetIndexSearcher(IndexSearcher searcher, string publicName, bool wait)
        {
            var old = current;
            current = new IndexSearcherHoldingState(searcher, publicName, context.DatabaseName);
            
            if (old == null)
                return null;

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

            private readonly ConcurrentDictionary<Tuple<int, uint>, StringCollectionValue> docsCache = new ConcurrentDictionary<Tuple<int, uint>, StringCollectionValue>();

            private readonly ReaderWriterLockSlim rwls = new ReaderWriterLockSlim();
            private string databaseName;
            private string indexName;

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

        
            public IndexSearcherHoldingState(IndexSearcher indexSearcher, string publicName, string databaseName)
            {
                IndexSearcher = indexSearcher;
                this.databaseName = databaseName;
                indexName = publicName;
                MemoryStatistics.RegisterLowMemoryHandler(this);
            }

            public LowMemoryHandlerStatistics HandleLowMemory()
            {
                rwls.EnterWriteLock();
                var countItems = docsCache.Count;
                try
                {
                    docsCache.Clear();
                }
                finally
                {
                    rwls.ExitWriteLock();
                }
                return new LowMemoryHandlerStatistics
                {
                    Name = indexName,
                    DatabaseName = databaseName,
                    Summary = $"A documents cache with {countItems:#,#} items was cleared"
                };
            }

            public LowMemoryHandlerStatistics GetStats()
            {
                return new LowMemoryHandlerStatistics
                {
                    Name = "IndexSearcherHoldingState" ,
                    Metadata = new 
                    {
                        IndexName=indexName
                    },
                    DatabaseName = databaseName,
                    EstimatedUsedMemory = docsCache.Count*(sizeof(int) +sizeof(uint))*2
                };
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
    }
}
