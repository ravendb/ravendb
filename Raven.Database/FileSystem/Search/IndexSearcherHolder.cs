using System;
using System.Threading;
using Lucene.Net.Search;

namespace Raven.Database.FileSystem.Search
{
    public class IndexSearcherHolder
    {
        private volatile IndexSearcherHoldingState current;

        public void SetIndexSearcher(IndexSearcher searcher)
        {
            var old = current;
            current = new IndexSearcherHoldingState(searcher);

            if (old == null)
                return;

            Interlocked.Increment(ref old.Usage);
            using (old)
            {
                old.MarkForDisposal();
            }
        }

        public IDisposable GetSearcher(out IndexSearcher searcher)
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

                searcher = state.IndexSearcher;
                return state;
            }
        }

        private class IndexSearcherHoldingState : IDisposable
        {
            public readonly IndexSearcher IndexSearcher;

            public volatile bool ShouldDispose;
            public int Usage;

            public IndexSearcherHoldingState(IndexSearcher indexSearcher)
            {
                this.IndexSearcher = indexSearcher;
            }

            public void MarkForDisposal()
            {
                this.ShouldDispose = true;
            }

            public void Dispose()
            {
                if (Interlocked.Decrement(ref Usage) > 0)
                    return;
                if (this.ShouldDispose == false)
                    return;
                this.DisposeRudely();
            }

            private void DisposeRudely()
            {
                if (this.IndexSearcher != null)
                {
                    var indexReader = this.IndexSearcher.IndexReader;
                    if (indexReader != null)
                        indexReader.Dispose();
                    this.IndexSearcher.Dispose();
                }
            }
        }
    }
}
