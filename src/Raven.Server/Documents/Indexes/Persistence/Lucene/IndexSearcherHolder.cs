using System;
using System.Threading;

using Lucene.Net.Search;

using Raven.Abstractions.Logging;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public class IndexSearcherHolder
    {
        private readonly Func<IndexSearcher> _recreateSearcher;

        private static readonly ILog Log = LogManager.GetLogger(typeof(IndexSearcherHolder));

        private volatile IndexSearcherHoldingState _current;

        public IndexSearcherHolder(Func<IndexSearcher> recreateSearcher)
        {
            _recreateSearcher = recreateSearcher;
        }

        public ManualResetEvent SetIndexSearcher(bool wait)
        {
            var old = _current;
            _current = new IndexSearcherHoldingState(_recreateSearcher);

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
                searcher = indexSearcherHoldingState.IndexSearcher.Value;
                return indexSearcherHoldingState;
            }
            catch (Exception e)
            {
                Log.ErrorException("Failed to get the index searcher.", e);
                indexSearcherHoldingState.Dispose();
                throw;
            }
        }

        internal IndexSearcherHoldingState GetCurrentStateHolder()
        {
            while (true)
            {
                var state = _current;
                Interlocked.Increment(ref state.Usage);
                if (state.ShouldDispose)
                {
                    state.Dispose();
                    continue;
                }

                return state;
            }
        }


        internal class IndexSearcherHoldingState : IDisposable
        {
            private readonly Func<IndexSearcher> _recreateSearcher;

            public Lazy<IndexSearcher> IndexSearcher => new Lazy<IndexSearcher>(_recreateSearcher, LazyThreadSafetyMode.ExecutionAndPublication);

            public volatile bool ShouldDispose;
            public int Usage;
            private readonly Lazy<ManualResetEvent> _disposed = new Lazy<ManualResetEvent>(() => new ManualResetEvent(false));

            public IndexSearcherHoldingState(Func<IndexSearcher> recreateSearcher)
            {
                _recreateSearcher = recreateSearcher;
            }

            public void MarkForDisposal()
            {
                ShouldDispose = true;
            }

            public ManualResetEvent MarkForDisposalWithWait()
            {
                var x = _disposed.Value;//  first create the value
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
                if (IndexSearcher.IsValueCreated)
                {
                    using (IndexSearcher.Value)
                    using (IndexSearcher.Value.IndexReader) { }
                }
                if (_disposed.IsValueCreated)
                    _disposed.Value.Set();
            }

        }
    }
}