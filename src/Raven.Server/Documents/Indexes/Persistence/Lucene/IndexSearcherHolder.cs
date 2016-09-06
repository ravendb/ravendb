using System;
using System.Threading;
using Sparrow.Logging;
using Lucene.Net.Search;

using Raven.Abstractions.Logging;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public class IndexSearcherHolder
    {
        private readonly Func<IndexSearcher> _recreateSearcher;

        private Logger _logger;
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

        public IDisposable GetSearcher(out IndexSearcher searcher, DocumentDatabase documentDatabase)
        {
            _logger = LoggingSource.Instance.GetLogger<IndexSearcherHolder>(documentDatabase.Name);
            var indexSearcherHoldingState = GetCurrentStateHolder();
            try
            {
                searcher = indexSearcherHoldingState.IndexSearcher.Value;
                return indexSearcherHoldingState;
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Failed to get the index searcher.", e);
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
            public readonly Lazy<IndexSearcher> IndexSearcher;

            public volatile bool ShouldDispose;
            public int Usage;
            private readonly Lazy<ManualResetEvent> _disposed = new Lazy<ManualResetEvent>(() => new ManualResetEvent(false));

            public IndexSearcherHoldingState(Func<IndexSearcher> recreateSearcher)
            {
                IndexSearcher = new Lazy<IndexSearcher>(recreateSearcher, LazyThreadSafetyMode.ExecutionAndPublication);
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