using System;
using System.Threading;
using Lucene.Net.Search;
using Raven.Abstractions.Extensions;

namespace Raven.Database.Indexing
{
	public class IndexSearcherHolder
	{
		private readonly IndexSearcher currentIndexSearcher;

		private volatile bool shouldDispose;
		private int usage;

		public IndexSearcherHolder(IndexSearcher currentIndexSearcher)
		{
			this.currentIndexSearcher = currentIndexSearcher;
		}

		public IDisposable GetSearcher(out IndexSearcher searcher)
		{
			Interlocked.Increment(ref usage);
			searcher = currentIndexSearcher;
			return new DisposableAction(TryDispose);
		}

		public void DisposeSafely()
		{
			shouldDispose = true;
		}

		private void TryDispose()
		{
			if (Interlocked.Decrement(ref usage) > 0)
				return;
			if (shouldDispose == false)
				return;

			DisposeRudely();
		}

		public void DisposeRudely()
		{
			currentIndexSearcher.GetIndexReader().Close();
			currentIndexSearcher.Close();
		}
	}
}