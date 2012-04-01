using System;
using System.IO;
using System.Threading;
using Lucene.Net.Search;
using Raven.Abstractions.Extensions;

namespace Raven.Database.Indexing
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
				IndexSearcher = indexSearcher;
			}

			public void MarkForDisposal()
			{
				ShouldDispose = true;
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
				var indexReader = IndexSearcher.GetIndexReader();
				if (indexReader != null)
					indexReader.Close();
				IndexSearcher.Close();
			}
		}
	}
}