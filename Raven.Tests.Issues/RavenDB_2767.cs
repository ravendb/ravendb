// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2767.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Globalization;
using System.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Client.Embedded;
using Raven.Database.Indexing;
using Raven.Database.Prefetching;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2767 : RavenTest
	{
		private readonly EmbeddableDocumentStore store;
		private readonly PrefetchingBehavior prefetchingBehavior;

		public RavenDB_2767()
		{
			store = NewDocumentStore();
			var workContext = store.SystemDatabase.WorkContext;
			prefetchingBehavior = new PrefetchingBehavior(PrefetchingUser.Indexer, workContext, new IndexBatchSizeAutoTuner(workContext));
			prefetchingBehavior.ShouldHandleUnusedDocumentsAddedAfterCommit = true;
		}

		[Fact]
		public void ShouldDisableCollectingDocsAfterCommit()
		{
			Etag last = Etag.Empty;

			SystemTime.UtcDateTime = () => DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(15));

			prefetchingBehavior.AfterStorageCommitBeforeWorkNotifications(Enumerable.Range(0, 5).Select(x =>
			{
				last = EtagUtil.Increment(last, 1);

				return new JsonDocument
				{
					Etag = last,
					Key = x.ToString(CultureInfo.InvariantCulture)
				};
			}).ToArray());

			last = EtagUtil.Increment(last, store.Configuration.MaxNumberOfItemsToProcessInSingleBatch);

			prefetchingBehavior.AfterStorageCommitBeforeWorkNotifications(Enumerable.Range(0, 5).Select(x =>
			{
				last = EtagUtil.Increment(last, 1);

				return new JsonDocument
				{
					Etag = last,
					Key = x.ToString(CultureInfo.InvariantCulture)
				};
			}).ToArray());

			SystemTime.UtcDateTime = null;

			var documentsBatchFrom = prefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty);

			prefetchingBehavior.CleanupDocuments(documentsBatchFrom.Last().Etag);

			Assert.True(prefetchingBehavior.DisableCollectingDocumentsAfterCommit);

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

			Assert.Equal(0, prefetchingBehavior.InMemoryIndexingQueueSize);
		}

		[Fact]
		public void ShouldCleanUpDocsThatResideInQueueTooLong()
		{
			Etag last = Etag.Empty;

			SystemTime.UtcDateTime = () => DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(15));

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

			SystemTime.UtcDateTime = null;

			prefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty);
			prefetchingBehavior.CleanupDocuments(Etag.Empty);

			Assert.Equal(0, prefetchingBehavior.InMemoryIndexingQueueSize);
		}

		[Fact]
		public void ShouldEnableCollectingDocsAfterCommit()
		{
			Etag last = Etag.Empty;

			SystemTime.UtcDateTime = () => DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(15));

			last = EtagUtil.Increment(last, 1);
			prefetchingBehavior.AfterStorageCommitBeforeWorkNotifications(new[]
			{
				new JsonDocument
				{
					Etag = last,
					Key = "items/1"
				}
			});

			SystemTime.UtcDateTime = null;

			prefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty);
			prefetchingBehavior.CleanupDocuments(Etag.Empty);

			Assert.True(prefetchingBehavior.DisableCollectingDocumentsAfterCommit);

			prefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty.IncrementBy(5)); // will trigger check for enabling collecting docs again

			Assert.False(prefetchingBehavior.DisableCollectingDocumentsAfterCommit);

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

			Assert.Equal(5, prefetchingBehavior.InMemoryIndexingQueueSize);
		}
	}
}