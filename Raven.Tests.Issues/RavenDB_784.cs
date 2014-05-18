using System.Threading;

using Raven.Tests.Common;

namespace Raven.Tests.Issues
{
	using System.Collections.Generic;
	using System.Linq;
	using Database.Storage;
	using Raven.Json.Linq;
	using Xunit;
	using Xunit.Extensions;

	public class RavenDB_784 : RavenTest
	{
        int a = 100;
        int b = 200;

		[Theory]
        [PropertyData("Storages")]
		public void ShouldRemoveDataFromReduceKeysCountsOnIndexDelete(string storageType)
		{
			using (var storage = NewTransactionalStorage(requestedStorage: storageType))
			{
				storage.Batch(accessor =>
				{
					accessor.Indexing.AddIndex(a, true);
					accessor.Indexing.AddIndex(b, true);

					accessor.MapReduce.PutMappedResult(a, "a/1", "a", new RavenJObject());
					accessor.MapReduce.PutMappedResult(a, "a/2", "a", new RavenJObject());
					accessor.MapReduce.PutMappedResult(b, "a/1", "b", new RavenJObject());
					accessor.MapReduce.PutMappedResult(b, "a/1", "b", new RavenJObject());

					accessor.MapReduce.IncrementReduceKeyCounter(a, "a", 2);
					accessor.MapReduce.IncrementReduceKeyCounter(b, "b", 2);
				});

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce.GetKeysStats(a, 0, 10).ToList();
					Assert.Equal(1, results.Count);
					Assert.Equal(2, results[0].Count);

					results = accessor.MapReduce.GetKeysStats(b, 0, 10).ToList();
					Assert.Equal(1, results.Count);
					Assert.Equal(2, results[0].Count);
				});

                storage.Batch(accessor => accessor.Indexing.DeleteIndex(a, new CancellationToken()));

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce.GetKeysStats(a, 0, 10).ToList();
					Assert.Equal(0, results.Count());

					results = accessor.MapReduce.GetKeysStats(b, 0, 10).ToList();
					Assert.Equal(1, results.Count);
					Assert.Equal("b", results[0].Key);
					Assert.Equal(2, results[0].Count);
				});
			}
		}

		[Theory]
        [PropertyData("Storages")]
		public void ShouldRemoveDataFromReduceKeysCountsWhenReduceKeyIsGone(string storageType)
		{
			using (var storage = NewTransactionalStorage(requestedStorage: storageType))
			{
				storage.Batch(accessor =>
				{
					accessor.Indexing.AddIndex(a, true);

					accessor.MapReduce.PutMappedResult(a, "a/1", "a", new RavenJObject());
					accessor.MapReduce.PutMappedResult(a, "a/2", "a", new RavenJObject());
					accessor.MapReduce.PutMappedResult(a, "a/3", "b", new RavenJObject());
					accessor.MapReduce.PutMappedResult(a, "a/4", "b", new RavenJObject());

					accessor.MapReduce.IncrementReduceKeyCounter(a, "a", 2);
					accessor.MapReduce.IncrementReduceKeyCounter(a, "b", 2);

				});

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce.GetKeysStats(a, 0, 10).ToList();
					Assert.Equal(2, results.Count);
					Assert.Equal(2, results.First(x => x.Key == "a").Count);
					Assert.Equal(2, results.First(x => x.Key == "b").Count);
				});

				storage.Batch(accessor =>
				{
					var removed = new Dictionary<ReduceKeyAndBucket, int>();
					accessor.MapReduce.DeleteMappedResultsForDocumentId("a/3", a, removed);
					accessor.MapReduce.DeleteMappedResultsForDocumentId("a/4", a, removed);

					accessor.MapReduce.UpdateRemovedMapReduceStats(a, removed);
				});

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce.GetKeysStats(a, 0, 10).ToList();
					Assert.Equal(1, results.Count);
					Assert.Equal("a", results[0].Key);
					Assert.Equal(2, results[0].Count);
				});
			}
		}

		[Theory]
        [PropertyData("Storages")]
		public void ShouldRemoveDataFromReduceKeysCountsOnDeletingAllMappedResultsForView(string storageType)
		{
			using (var storage = NewTransactionalStorage(requestedStorage: storageType))
			{
				storage.Batch(accessor =>
				{
					accessor.Indexing.AddIndex(a, true);

					accessor.MapReduce.PutMappedResult(a, "a/1", "a", new RavenJObject());
					accessor.MapReduce.PutMappedResult(a, "a/2", "a", new RavenJObject());
					accessor.MapReduce.PutMappedResult(a, "a/3", "b", new RavenJObject());
					accessor.MapReduce.PutMappedResult(a, "a/4", "b", new RavenJObject());

					accessor.MapReduce.IncrementReduceKeyCounter(a, "a", 2);
					accessor.MapReduce.IncrementReduceKeyCounter(a, "b", 2);
				});

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce.GetKeysStats(a, 0, 10).ToList();
					Assert.Equal(2, results.Count);
					Assert.Equal(2, results.First(x => x.Key == "a").Count);
					Assert.Equal(2, results.First(x => x.Key == "b").Count);
				});

				storage.Batch(accessor => accessor.MapReduce.DeleteMappedResultsForView(a));

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce.GetKeysStats(a, 0, 10).ToList();
					Assert.Equal(0, results.Count);
				});
			}
		}

		[Theory]
        [PropertyData("Storages")]
		public void ShouldRemoveDataFromReduceKeysStatusOnIndexDelete(string storageType)
		{
			using (var storage = NewTransactionalStorage(requestedStorage: storageType))
			{
				storage.Batch(accessor =>
				{
					accessor.Indexing.AddIndex(a, true);
					accessor.Indexing.AddIndex(b, true);

					accessor.MapReduce.PutMappedResult(a, "a/1", "a", new RavenJObject());
					accessor.MapReduce.PutMappedResult(a, "a/2", "a", new RavenJObject());
					accessor.MapReduce.PutMappedResult(b, "a/1", "b", new RavenJObject());
					accessor.MapReduce.PutMappedResult(b, "a/1", "b", new RavenJObject());

					accessor.MapReduce.IncrementReduceKeyCounter(a, "a", 2);
					accessor.MapReduce.IncrementReduceKeyCounter(a, "b", 2);
				});

				storage.Batch(accessor =>
				{
					accessor.MapReduce.UpdatePerformedReduceType(a, "a", ReduceType.SingleStep);
					accessor.MapReduce.UpdatePerformedReduceType(b, "b", ReduceType.SingleStep);
				});

                storage.Batch(accessor => accessor.Indexing.DeleteIndex(a, new CancellationToken()));

				storage.Batch(accessor =>
				{
					var result = accessor.MapReduce.GetLastPerformedReduceType(a, "a");

					Assert.Equal(ReduceType.None, result);

					result = accessor.MapReduce.GetLastPerformedReduceType(b, "b");
					Assert.Equal(ReduceType.SingleStep, result);
				});
			}
		}

		[Theory]
        [PropertyData("Storages")]
		public void ShouldRemoveDataFromReduceKeysStatusWhenReduceKeyIsGone(string storageType)
		{
			using (var storage = NewTransactionalStorage(requestedStorage: storageType))
			{
				storage.Batch(accessor =>
				{
					accessor.Indexing.AddIndex(a, true);

					accessor.MapReduce.PutMappedResult(a, "a/1", "a", new RavenJObject());
					accessor.MapReduce.PutMappedResult(a, "a/2", "a", new RavenJObject());
					accessor.MapReduce.PutMappedResult(a, "a/3", "b", new RavenJObject());
					accessor.MapReduce.PutMappedResult(a, "a/4", "b", new RavenJObject());

					accessor.MapReduce.IncrementReduceKeyCounter(a, "a", 2);
					accessor.MapReduce.IncrementReduceKeyCounter(a, "b", 2);
				});

				storage.Batch(accessor =>
				{
					accessor.MapReduce.UpdatePerformedReduceType(a, "a", ReduceType.SingleStep);
					accessor.MapReduce.UpdatePerformedReduceType(a, "b", ReduceType.SingleStep);
				});

				storage.Batch(accessor =>
				{
					var removed = new Dictionary<ReduceKeyAndBucket, int>();
					accessor.MapReduce.DeleteMappedResultsForDocumentId("a/3", a, removed);
					accessor.MapReduce.DeleteMappedResultsForDocumentId("a/4", a, removed);
					accessor.MapReduce.UpdateRemovedMapReduceStats(a, removed);
				});

				storage.Batch(accessor =>
				{
					var result = accessor.MapReduce.GetLastPerformedReduceType(a, "a");
					Assert.Equal(ReduceType.SingleStep, result);

					result = accessor.MapReduce.GetLastPerformedReduceType(a, "b");
					Assert.Equal(ReduceType.None, result);
				});
			}
		}
	}
}
