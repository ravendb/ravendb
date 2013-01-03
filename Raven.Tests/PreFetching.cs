using System;
using System.Globalization;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Client.Embedded;
using Raven.Database.Indexing;
using Xunit;

namespace Raven.Tests
{
	public class PreFetching : RavenTest
	{
		private readonly EmbeddableDocumentStore store;
		private readonly PrefetchingBehavior prefetchingBehavior;

		public PreFetching()
		{
			store = NewDocumentStore();
				var workContext = store.DocumentDatabase.WorkContext;
			prefetchingBehavior = new PrefetchingBehavior(workContext, new IndexBatchSizeAutoTuner(workContext));
		}

		public override void Dispose()
		{
			store.Dispose();
			base.Dispose();
		}

		[Fact]
		public void CanMergeConsecutiveInMemoryUpdates()
		{
			Etag last = Etag.Empty;
			for (int i = 0; i < 5; i++)
			{
				last = EtagUtil.Increment(last, 1);
				prefetchingBehavior.AfterStorageCommitBeforeWorkNotifications(new[]
				{
					new JsonDocument
					{
						Etag = last,
						Key = i.ToString(CultureInfo.InvariantCulture)
					},
				});
			}

			Assert.Equal(5, prefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty).Count);
		}

		[Fact]
		public void CanProperlyHandleNonConsecutiveUpdates()
		{
			Etag last = Etag.Empty;
			for (int i = 0; i < 5; i++)
			{
				last = EtagUtil.Increment(last, 1);
				prefetchingBehavior.AfterStorageCommitBeforeWorkNotifications(new[]
				{
					new JsonDocument
					{
						Etag = last,
						Key = i.ToString(CultureInfo.InvariantCulture)
					},
				});
			}
			last = EtagUtil.Increment(last, 10);
			for (int i = 0; i < 5; i++)
			{
				last = EtagUtil.Increment(last, 1);
				prefetchingBehavior.AfterStorageCommitBeforeWorkNotifications(new[]
				{
					new JsonDocument
					{
						Etag = last,
						Key = i.ToString(CultureInfo.InvariantCulture)
					},
				});
			}

			Assert.Equal(5, prefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty).Count);
			Assert.Equal(5, prefetchingBehavior.GetDocumentsBatchFrom(EtagUtil.Increment(Etag.Empty, 15)).Count);
		}
	}
}