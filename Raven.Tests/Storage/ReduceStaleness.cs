using System;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Indexing;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Json.Linq;
using Xunit;
using System.Linq;

namespace Raven.Tests.Storage
{
	public class ReduceStaleness : RavenTest
	{
		[Fact]
		public void when_there_are_multiple_map_results_for_multiple_indexes()
		{
			using(var transactionalStorage = NewTransactionalStorage())
			{
				transactionalStorage.Batch(accessor =>
				{
					accessor.Indexing.AddIndex("a", true);
					accessor.Indexing.AddIndex("b", true);
					accessor.Indexing.AddIndex("c", true);

					accessor.MapReduce.ScheduleReductions("a", 0, new[] {new ReduceKeyAndBucket(0, "a"),});
					accessor.MapReduce.ScheduleReductions("b", 0, new[] {new ReduceKeyAndBucket(0, "a"),});
					accessor.MapReduce.ScheduleReductions("c", 0, new[] {new ReduceKeyAndBucket(0, "a"),});
				});

				transactionalStorage.Batch(actionsAccessor =>
				{
					Assert.True(actionsAccessor.Staleness.IsReduceStale("a"));
					Assert.True(actionsAccessor.Staleness.IsReduceStale("b"));
					Assert.True(actionsAccessor.Staleness.IsReduceStale("c"));
				});
			}
		}

		[Fact]
		public void when_there_are_multiple_map_results_and_we_ask_for_results()
		{
			using (var transactionalStorage = NewTransactionalStorage())
			{
				transactionalStorage.Batch(accessor =>
				{
					accessor.Indexing.AddIndex("a", true);
					accessor.Indexing.AddIndex("b", true);
					accessor.Indexing.AddIndex("c", true);

					accessor.MapReduce.PutMappedResult("a", "a/1", "a", new RavenJObject());
					accessor.MapReduce.PutMappedResult("a", "a/2", "a", new RavenJObject());
					accessor.MapReduce.PutMappedResult("b", "a/1", "a", new RavenJObject());
					accessor.MapReduce.PutMappedResult("b", "a/1", "a", new RavenJObject());
					accessor.MapReduce.PutMappedResult("c", "a/1", "a", new RavenJObject());
					accessor.MapReduce.PutMappedResult("c", "a/1", "a", new RavenJObject());
				});

				transactionalStorage.Batch(actionsAccessor =>
				{
					Assert.Equal(1, actionsAccessor.MapReduce.GetMappedResultsReduceKeysAfter("a", Guid.Empty, false, 100).Count());
					Assert.Equal(1, actionsAccessor.MapReduce.GetMappedResultsReduceKeysAfter("b", Guid.Empty, false, 100).Count());
					Assert.Equal(1, actionsAccessor.MapReduce.GetMappedResultsReduceKeysAfter("c", Guid.Empty, false, 100).Count());
				});
			}
		}

		[Fact]
		public void when_there_are_updates_to_map_reduce_results()
		{
			using(var transactionalStorage = NewTransactionalStorage())
			{
				var dummyUuidGenerator = new DummyUuidGenerator();
				Guid a = Guid.Empty;
				Guid b = Guid.Empty;
				Guid c = Guid.Empty;
				transactionalStorage.Batch(accessor =>
				{
					accessor.Indexing.AddIndex("a", true);
					accessor.Indexing.AddIndex("b", true);
					accessor.Indexing.AddIndex("c", true);

					accessor.MapReduce.PutMappedResult("a", "a/1", "a", new RavenJObject());
					a = dummyUuidGenerator.CreateSequentialUuid();
					accessor.MapReduce.PutMappedResult("a", "a/2", "a", new RavenJObject());
					accessor.MapReduce.PutMappedResult("b", "a/1", "a", new RavenJObject());
					b = dummyUuidGenerator.CreateSequentialUuid();
					accessor.MapReduce.PutMappedResult("b", "a/1", "a", new RavenJObject());
					accessor.MapReduce.PutMappedResult("c", "a/1", "a", new RavenJObject());
					c = dummyUuidGenerator.CreateSequentialUuid();
					accessor.MapReduce.PutMappedResult("c", "a/1", "a", new RavenJObject());
				});

				transactionalStorage.Batch(actionsAccessor =>
				{
					Assert.Equal(1, actionsAccessor.MapReduce.GetMappedResultsReduceKeysAfter("a", a, false, 100).Count());
					Assert.Equal(1, actionsAccessor.MapReduce.GetMappedResultsReduceKeysAfter("b", b, false, 100).Count());
					Assert.Equal(1, actionsAccessor.MapReduce.GetMappedResultsReduceKeysAfter("c", c, false, 100).Count());
				});
			}
		}
	}
}