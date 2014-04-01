using System.Globalization;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Client.Embedded;
using Raven.Database.Indexing;
using Raven.Database.Prefetching;
using Raven.Tests.Common;

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

		[Fact]
		public void ShouldReturnDocsOnlyIfTheFirstEtagInQueueMatches()
		{
			Etag last = Etag.Empty.IncrementBy(5); // start from 5
			for (int i = 0; i < 5; i++)
			{
				last = EtagUtil.Increment(last, 1);
				prefetchingBehavior.AfterStorageCommitBeforeWorkNotifications(new[]
				{
					new JsonDocument
					{
						Etag = last,
						Key = i.ToString(CultureInfo.InvariantCulture) + "/1"
					},
				});
			}

			Assert.Equal(0, prefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty).Count);

			last = Etag.Empty; // now add missing docs from etag 0

			for (int i = 0; i < 5; i++)
			{
				last = EtagUtil.Increment(last, 1);
				prefetchingBehavior.AfterStorageCommitBeforeWorkNotifications(new[]
				{
					new JsonDocument
					{
						Etag = last,
						Key = i.ToString(CultureInfo.InvariantCulture) + "/2"
					},
				});
			}

			Assert.Equal(10, prefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty).Count);
		}

		[Fact]
		public void ShouldHandleDelete()
		{
			Etag first = Etag.Empty.IncrementBy(1);

			prefetchingBehavior.AfterStorageCommitBeforeWorkNotifications(new[]
			{
				new JsonDocument
				{
					Etag = first,
					Key = "1"
				},
			});

			Etag second = Etag.Empty.IncrementBy(2);

			prefetchingBehavior.AfterStorageCommitBeforeWorkNotifications(new[]
			{
				new JsonDocument
				{
					Etag = second,
					Key = "2"
				},
			});

			prefetchingBehavior.AfterDelete("1", first);

			Assert.Equal(1, prefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty).Count(x => prefetchingBehavior.FilterDocuments(x)));
		}

		[Fact]
		public void ShouldFilterDeletedDocs()
		{
			Etag first = Etag.Empty.IncrementBy(1);

			prefetchingBehavior.AfterStorageCommitBeforeWorkNotifications(new[]
			{
				new JsonDocument
				{
					Etag = first,
					Key = "1"
				},
			});

			Etag second = Etag.Empty.IncrementBy(2);

			prefetchingBehavior.AfterStorageCommitBeforeWorkNotifications(new[]
			{
				new JsonDocument
				{
					Etag = second,
					Key = "2"
				},
			});

			Etag third = Etag.Empty.IncrementBy(3);

			prefetchingBehavior.AfterStorageCommitBeforeWorkNotifications(new[]
			{
				new JsonDocument
				{
					Etag = third,
					Key = "3"
				},
			});

			Etag fourth = Etag.Empty.IncrementBy(4);

			prefetchingBehavior.AfterStorageCommitBeforeWorkNotifications(new[]
			{
				new JsonDocument
				{
					Etag = fourth,
					Key = "4"
				},
			});

			prefetchingBehavior.AfterDelete("1", first);
			prefetchingBehavior.AfterDelete("3", third);

			Assert.Equal(2, prefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty).Count(x => prefetchingBehavior.FilterDocuments(x)));
		}
	}
}