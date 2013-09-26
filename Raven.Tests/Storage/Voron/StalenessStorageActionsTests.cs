// -----------------------------------------------------------------------
//  <copyright file="StalenessStorageActionsTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Tests.Storage.Voron
{
	using System;
	using System.Collections.Generic;
	using System.IO;
	using System.Text;

	using Raven.Abstractions.Data;
	using Raven.Abstractions.Exceptions;
	using Raven.Database.Storage;
	using Raven.Database.Tasks;
	using Raven.Json.Linq;

	using Xunit;
	using Xunit.Extensions;

	public class StalenessStorageActionsTests : TransactionalStorageTestBase
	{
		[Theory]
		[PropertyData("Storages")]
		public void GetIndexTouchCount(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor =>
				{
					var count = accessor.Staleness.GetIndexTouchCount("index1");
					Assert.Equal(-1, count);
				});

				storage.Batch(accessor => accessor.Indexing.AddIndex("index1", true));

				storage.Batch(accessor =>
				{
					var count = accessor.Staleness.GetIndexTouchCount("index1");
					Assert.Equal(0, count);
				});

				storage.Batch(accessor => accessor.Indexing.TouchIndexEtag("index1"));
				storage.Batch(accessor => accessor.Indexing.TouchIndexEtag("index1"));

				storage.Batch(accessor =>
				{
					var count = accessor.Staleness.GetIndexTouchCount("index1");
					Assert.Equal(2, count);
				});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void GetMostRecentAttachmentEtag(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor => Assert.Equal(Etag.Empty, accessor.Staleness.GetMostRecentAttachmentEtag()));

				var etag1 = Etag.Parse("00000000-0000-0000-0000-000000000001");
				var etag2 = Etag.Parse("00000000-0000-0000-0000-000000000002");
				var etag3 = Etag.Parse("00000000-0000-0000-0000-000000000003");

				storage.Batch(accessor => accessor.Attachments.AddAttachment("key1", etag1, StreamFor("123"), new RavenJObject()));

				storage.Batch(accessor => Assert.Equal(etag1, accessor.Staleness.GetMostRecentAttachmentEtag()));

				storage.Batch(accessor =>
				{
					accessor.Attachments.AddAttachment("key2", etag2, StreamFor("123"), new RavenJObject());
					accessor.Attachments.AddAttachment("key3", etag3, StreamFor("123"), new RavenJObject());
				});

				storage.Batch(accessor => Assert.Equal(etag3, accessor.Staleness.GetMostRecentAttachmentEtag()));
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void GetMostRecentDocumentEtag(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor => Assert.Equal(Etag.Empty, accessor.Staleness.GetMostRecentDocumentEtag()));

				var etag1 = Etag.Parse("00000000-0000-0000-0000-000000000001");
				var etag2 = Etag.Parse("00000000-0000-0000-0000-000000000002");
				var etag3 = Etag.Parse("00000000-0000-0000-0000-000000000003");

				storage.Batch(accessor => accessor.Documents.AddDocument("key1", Etag.Empty, new RavenJObject(), new RavenJObject()));

				storage.Batch(accessor => Assert.Equal(etag1, accessor.Staleness.GetMostRecentDocumentEtag()));

				storage.Batch(accessor =>
				{
					accessor.Documents.AddDocument("key2", Etag.Empty, new RavenJObject(), new RavenJObject());
					accessor.Documents.AddDocument("key3", Etag.Empty, new RavenJObject(), new RavenJObject());
				});

				storage.Batch(accessor => Assert.Equal(etag3, accessor.Staleness.GetMostRecentDocumentEtag()));
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void IndexLastUpdatedAt(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				var etag1 = Etag.Parse("00000000-0000-0000-0000-000000000001");
				var etag2 = Etag.Parse("00000000-0000-0000-0000-000000000002");
				var etag3 = Etag.Parse("00000000-0000-0000-0000-000000000003");

				var date1 = DateTime.Now;
				var date2 = DateTime.Now.AddSeconds(100);
				var date3 = DateTime.Now.AddSeconds(1000);

				storage.Batch(accessor => Assert.Throws<IndexDoesNotExistsException>(() => accessor.Staleness.IndexLastUpdatedAt("index1")));

				storage.Batch(accessor =>
				{
					accessor.Indexing.AddIndex("index1", false);
					accessor.Indexing.AddIndex("index2", true);
				});

				storage.Batch(accessor =>
				{
					var r1 = accessor.Staleness.IndexLastUpdatedAt("index1");
					var r2 = accessor.Staleness.IndexLastUpdatedAt("index2");

					Assert.Equal(DateTime.MinValue, r1.Item1);
					Assert.Equal(Etag.Empty, r1.Item2);

					Assert.Equal(DateTime.MinValue, r2.Item1);
					Assert.Equal(Etag.Empty, r2.Item2);
				});

				storage.Batch(accessor =>
				{
					accessor.Indexing.UpdateLastIndexed("index1", etag1, date1);
					accessor.Indexing.UpdateLastIndexed("index2", etag2, date2);
				});

				storage.Batch(accessor =>
				{
					var r1 = accessor.Staleness.IndexLastUpdatedAt("index1");
					var r2 = accessor.Staleness.IndexLastUpdatedAt("index2");

					Assert.Equal(date1, r1.Item1);
					Assert.Equal(etag1, r1.Item2);

					Assert.Equal(DateTime.MinValue, r2.Item1);
					Assert.Equal(Etag.Empty, r2.Item2);
				});

				storage.Batch(accessor => accessor.Indexing.UpdateLastReduced("index2", etag3, date3));

				storage.Batch(accessor =>
				{
					var r2 = accessor.Staleness.IndexLastUpdatedAt("index2");

					Assert.Equal(date3, r2.Item1);
					Assert.Equal(etag3, r2.Item2);
				});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void IsMapStale(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor => Assert.False(accessor.Staleness.IsMapStale("index1")));

				var etag1 = Etag.Parse("00000000-0000-0000-0000-000000000001");
				var etag2 = Etag.Parse("00000000-0000-0000-0000-000000000002");
				var etag3 = Etag.Parse("00000000-0000-0000-0000-000000000003");

				var date1 = DateTime.Now;
				var date2 = DateTime.Now.AddSeconds(100);
				var date3 = DateTime.Now.AddSeconds(1000);

				storage.Batch(accessor =>
				{
					accessor.Indexing.AddIndex("index1", false);
					accessor.Indexing.AddIndex("index2", true);
				});

				storage.Batch(accessor =>
				{
					Assert.False(accessor.Staleness.IsMapStale("index1"));
					Assert.False(accessor.Staleness.IsMapStale("index2"));
				});

				storage.Batch(accessor => accessor.Documents.AddDocument("key1", Etag.Empty, new RavenJObject(), new RavenJObject()));

				storage.Batch(accessor =>
				{
					Assert.True(accessor.Staleness.IsMapStale("index1"));
					Assert.True(accessor.Staleness.IsMapStale("index2"));
				});

				storage.Batch(accessor =>
				{
					accessor.Indexing.UpdateLastIndexed("index1", etag1, date1);
					accessor.Indexing.UpdateLastIndexed("index2", etag1, date1);
				});

				storage.Batch(accessor =>
				{
					Assert.False(accessor.Staleness.IsMapStale("index1"));
					Assert.False(accessor.Staleness.IsMapStale("index2"));
				});

				storage.Batch(accessor => accessor.Documents.AddDocument("key2", Etag.Empty, new RavenJObject(), new RavenJObject()));

				storage.Batch(accessor =>
				{
					Assert.True(accessor.Staleness.IsMapStale("index1"));
					Assert.True(accessor.Staleness.IsMapStale("index2"));
				});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void IsReduceStale(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor => Assert.False(accessor.Staleness.IsReduceStale("index1")));

				storage.Batch(accessor => accessor.MapReduce.ScheduleReductions("index1", 0, new ReduceKeyAndBucket(1, "reduceKey1")));

				storage.Batch(accessor => Assert.True(accessor.Staleness.IsReduceStale("index1")));

				storage.Batch(accessor => accessor.MapReduce.DeleteScheduledReduction("index1", 0, "reduceKey1"));

				storage.Batch(accessor => Assert.False(accessor.Staleness.IsReduceStale("index1")));
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void IsIndexStale1(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				var etag1 = Etag.Parse("00000000-0000-0000-0000-000000000001");
				var etag2 = Etag.Parse("00000000-0000-0000-0000-000000000002");
				var etag3 = Etag.Parse("00000000-0000-0000-0000-000000000003");

				storage.Batch(accessor => Assert.False(accessor.Staleness.IsIndexStale("index1", null, null)));

				storage.Batch(accessor =>
				{
					accessor.Indexing.AddIndex("index1", false);
					accessor.Documents.AddDocument("key1", Etag.Empty, new RavenJObject(), new RavenJObject());
					accessor.Documents.AddDocument("key2", Etag.Empty, new RavenJObject(), new RavenJObject());
					accessor.Documents.AddDocument("key3", Etag.Empty, new RavenJObject(), new RavenJObject());
				});

				var date0 = DateTime.Now.AddSeconds(-10);
				var date1 = DateTime.Now;
				var date2 = DateTime.Now.AddSeconds(100);
				var date3 = DateTime.Now.AddSeconds(1000);

				storage.Batch(accessor => Assert.True(accessor.Staleness.IsIndexStale("index1", null, null)));
				storage.Batch(accessor => accessor.Indexing.UpdateLastIndexed("index1", etag2, date2));

				storage.Batch(accessor =>
				{
					Assert.False(accessor.Staleness.IsIndexStale("index1", date0, null));
					Assert.False(accessor.Staleness.IsIndexStale("index1", date1, null));
					Assert.True(accessor.Staleness.IsIndexStale("index1", date2, null));

					Assert.False(accessor.Staleness.IsIndexStale("index1", null, etag1));
					Assert.False(accessor.Staleness.IsIndexStale("index1", null, etag2));
					Assert.True(accessor.Staleness.IsIndexStale("index1", null, etag3));
				});

				storage.Batch(accessor =>
				{
					accessor.Indexing.UpdateLastIndexed("index1", etag3, date3);
					accessor.Tasks.AddTask(new RemoveFromIndexTask
										   {
											   Index = "index1",
											   Keys = new HashSet<string>()
										   }, date1);
				});

				storage.Batch(accessor =>
				{
					Assert.True(accessor.Staleness.IsIndexStale("index1", null, null));
					Assert.False(accessor.Staleness.IsIndexStale("index1", date0, null));
					Assert.True(accessor.Staleness.IsIndexStale("index1", date1, null));
					Assert.True(accessor.Staleness.IsIndexStale("index1", date2, null));
				});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void IsIndexStale2(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				var etag1 = Etag.Parse("00000000-0000-0000-0000-000000000001");
				var etag2 = Etag.Parse("00000000-0000-0000-0000-000000000002");
				var etag3 = Etag.Parse("00000000-0000-0000-0000-000000000003");

				storage.Batch(accessor => Assert.False(accessor.Staleness.IsIndexStale("index1", null, null)));

				storage.Batch(accessor =>
				{
					accessor.Indexing.AddIndex("index1", true);
					accessor.MapReduce.ScheduleReductions("index1", 0, new ReduceKeyAndBucket(1, "reduceKey1"));
				});

				var date0 = DateTime.Now.AddSeconds(-10);
				var date1 = DateTime.Now;
				var date2 = DateTime.Now.AddSeconds(100);
				var date3 = DateTime.Now.AddSeconds(1000);

				storage.Batch(accessor => Assert.True(accessor.Staleness.IsIndexStale("index1", null, null)));
				storage.Batch(accessor =>
				{ 
					accessor.Indexing.UpdateLastIndexed("index1", etag2, date2);
					accessor.Indexing.UpdateLastReduced("index1", etag2, date2);
				});

				storage.Batch(accessor =>
				{
					Assert.False(accessor.Staleness.IsIndexStale("index1", date0, null));
					Assert.False(accessor.Staleness.IsIndexStale("index1", date1, null));
					Assert.True(accessor.Staleness.IsIndexStale("index1", date2, null));

					Assert.False(accessor.Staleness.IsIndexStale("index1", null, etag1));
					Assert.False(accessor.Staleness.IsIndexStale("index1", null, etag2));
					Assert.True(accessor.Staleness.IsIndexStale("index1", null, etag3));
				});

				storage.Batch(accessor =>
				{
					accessor.MapReduce.DeleteScheduledReduction("index1", 0, "reduceKey1");
					accessor.Tasks.AddTask(new RemoveFromIndexTask
					{
						Index = "index1",
						Keys = new HashSet<string>()
					}, date1);
				});

				storage.Batch(accessor =>
				{
					Assert.True(accessor.Staleness.IsIndexStale("index1", null, null));
					Assert.False(accessor.Staleness.IsIndexStale("index1", date0, null));
					Assert.True(accessor.Staleness.IsIndexStale("index1", date1, null));
					Assert.True(accessor.Staleness.IsIndexStale("index1", date2, null));
				});
			}
		}

		private Stream StreamFor(string val)
		{
			return new MemoryStream(Encoding.UTF8.GetBytes(val));
		}
	}
}