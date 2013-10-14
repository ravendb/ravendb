// -----------------------------------------------------------------------
//  <copyright file="MappedResultsStorageActionsTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Tests.Storage.Voron
{
	using System;
	using System.Collections.Generic;
	using System.Linq;

	using Raven.Abstractions;
	using Raven.Abstractions.Data;
	using Raven.Database.Indexing;
	using Raven.Database.Storage;
	using Raven.Json.Linq;

	using Xunit;
	using Xunit.Extensions;

	[Trait("VoronTest", "StorageActionsTests")]
	public class MappedResultsStorageActionsTests : TransactionalStorageTestBase
	{
		[Theory]
		[PropertyData("Storages")]
		public void IncrementReduceKeyCounterWithNegativeValues(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor => accessor.MapReduce.IncrementReduceKeyCounter(303, "reduceKey1", 11));

				storage.Batch(
					accessor =>
					{
						var reduceKeysAndTypes = accessor.MapReduce.GetReduceKeysAndTypes(303, 0, 10).ToList();
						var keyStats = accessor.MapReduce.GetKeysStats(303, 0, 10).ToList();

						Assert.Equal(0, reduceKeysAndTypes.Count);
						Assert.Equal(1, keyStats.Count);
					});

				storage.Batch(accessor => accessor.MapReduce.IncrementReduceKeyCounter(303, "reduceKey1", 0));

				storage.Batch(
					accessor =>
					{
						var keyStats = accessor.MapReduce.GetKeysStats(303, 0, 10).ToList();

						Assert.Equal(1, keyStats.Count);

						var k1 = keyStats[0];
						Assert.Equal(11, k1.Count);
					});

				storage.Batch(accessor => accessor.MapReduce.IncrementReduceKeyCounter(303, "reduceKey1", -1));

				storage.Batch(
					accessor =>
					{
						var keyStats = accessor.MapReduce.GetKeysStats(303, 0, 10).ToList();

						Assert.Equal(1, keyStats.Count);

						var k1 = keyStats[0];
						Assert.Equal(10, k1.Count);
					});

				storage.Batch(accessor => accessor.MapReduce.IncrementReduceKeyCounter(303, "reduceKey1", -10));

				storage.Batch(
					accessor =>
					{
						var keyStats = accessor.MapReduce.GetKeysStats(303, 0, 10).ToList();

						Assert.Equal(1, keyStats.Count);

						var k1 = keyStats[0];
						Assert.Equal(0, k1.Count);
					});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void IncrementReduceKeyCounter(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor => accessor.MapReduce.IncrementReduceKeyCounter(303, "reduceKey1", 7));

				storage.Batch(accessor =>
				{
					var reduceKeysAndTypes = accessor.MapReduce.GetReduceKeysAndTypes(303, 0, 10).ToList();
					Assert.Equal(0, reduceKeysAndTypes.Count);

					var keyStats = accessor.MapReduce.GetKeysStats(303, 0, 10).ToList();
					Assert.Equal(1, keyStats.Count);

					var k1 = keyStats[0];
					Assert.Equal("reduceKey1", k1.Key);
					Assert.Equal(7, k1.Count);
				});

				storage.Batch(accessor => accessor.MapReduce.IncrementReduceKeyCounter(303, "reduceKey1", 3));

				storage.Batch(accessor =>
				{
					var reduceKeysAndTypes = accessor.MapReduce.GetReduceKeysAndTypes(303, 0, 10).ToList();
					Assert.Equal(0, reduceKeysAndTypes.Count);

					var keyStats = accessor.MapReduce.GetKeysStats(303, 0, 10).ToList();
					Assert.Equal(1, keyStats.Count);

					var k1 = keyStats[0];
					Assert.Equal("reduceKey1", k1.Key);
					Assert.Equal(10, k1.Count);
				});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void GetReduceKeysAndTypes(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor => Assert.Equal(0, accessor.MapReduce.GetReduceKeysAndTypes(303, 0, 10).Count()));

				storage.Batch(accessor => accessor.MapReduce.UpdatePerformedReduceType(303, "reduceKey1", ReduceType.SingleStep));
				storage.Batch(accessor => Assert.Equal(1, accessor.MapReduce.GetReduceKeysAndTypes(303, 0, 10).Count()));

				storage.Batch(accessor => accessor.MapReduce.UpdatePerformedReduceType(303, "reduceKey1", ReduceType.SingleStep));
				storage.Batch(accessor => Assert.Equal(1, accessor.MapReduce.GetReduceKeysAndTypes(303, 0, 10).Count()));

				storage.Batch(accessor => accessor.MapReduce.UpdatePerformedReduceType(303, "reduceKey2", ReduceType.SingleStep));
				storage.Batch(accessor => accessor.MapReduce.UpdatePerformedReduceType(303, "reduceKey3", ReduceType.SingleStep));
				storage.Batch(accessor => accessor.MapReduce.UpdatePerformedReduceType(404, "reduceKey4", ReduceType.MultiStep));
				storage.Batch(accessor => Assert.Equal(3, accessor.MapReduce.GetReduceKeysAndTypes(303, 0, 10).Count()));

				if (requestedStorage == "esent")
				{
					storage.Batch(accessor => accessor.MapReduce.PutMappedResult(303, "doc1", "reduceKey1", new RavenJObject()));
					storage.Batch(accessor => accessor.MapReduce.PutMappedResult(303, "doc2", "reduceKey1", new RavenJObject()));
					storage.Batch(accessor => accessor.MapReduce.PutMappedResult(303, "doc3", "reduceKey1", new RavenJObject()));
					storage.Batch(accessor => accessor.MapReduce.PutMappedResult(303, "doc4", "reduceKey1", new RavenJObject()));
					storage.Batch(accessor => accessor.MapReduce.PutMappedResult(303, "doc5", "reduceKey1", new RavenJObject()));
				}

				storage.Batch(accessor =>
				{
					var reduceKeyAndTypes = accessor.MapReduce.GetReduceKeysAndTypes(303, 0, 1).ToList();
					Assert.Equal(1, reduceKeyAndTypes.Count);
					var k1 = reduceKeyAndTypes[0];

					reduceKeyAndTypes = accessor.MapReduce.GetReduceKeysAndTypes(303, 1, 1).ToList();
					Assert.Equal(1, reduceKeyAndTypes.Count);
					var k2 = reduceKeyAndTypes[0];

					reduceKeyAndTypes = accessor.MapReduce.GetReduceKeysAndTypes(303, 2, 1).ToList();
					Assert.Equal(1, reduceKeyAndTypes.Count);
					var k3 = reduceKeyAndTypes[0];

					Assert.NotEqual(k1.ReduceKey, k2.ReduceKey);
					Assert.NotEqual(k1.ReduceKey, k3.ReduceKey);
					Assert.NotEqual(k2.ReduceKey, k3.ReduceKey);

					reduceKeyAndTypes = accessor.MapReduce.GetReduceKeysAndTypes(303, 1, 2).ToList();
					Assert.Equal(2, reduceKeyAndTypes.Count);
					Assert.Equal(k2.ReduceKey, reduceKeyAndTypes[0].ReduceKey);
					Assert.Equal(k3.ReduceKey, reduceKeyAndTypes[1].ReduceKey);

					reduceKeyAndTypes = accessor.MapReduce.GetReduceKeysAndTypes(303, 2, 2).ToList();
					Assert.Equal(1, reduceKeyAndTypes.Count);
					Assert.Equal(k3.ReduceKey, reduceKeyAndTypes[0].ReduceKey);

					reduceKeyAndTypes = accessor.MapReduce.GetReduceKeysAndTypes(303, 0, 5).ToList();
					Assert.Equal(3, reduceKeyAndTypes.Count);
					Assert.Equal(k1.ReduceKey, reduceKeyAndTypes[0].ReduceKey);
					Assert.Equal(k2.ReduceKey, reduceKeyAndTypes[1].ReduceKey);
					Assert.Equal(k3.ReduceKey, reduceKeyAndTypes[2].ReduceKey);

					reduceKeyAndTypes = accessor.MapReduce.GetReduceKeysAndTypes(303, 5, 55).ToList();
					Assert.Equal(0, reduceKeyAndTypes.Count);

					reduceKeyAndTypes = accessor.MapReduce.GetReduceKeysAndTypes(404, 0, 10).ToList();
					Assert.Equal(1, reduceKeyAndTypes.Count);
					Assert.Equal("reduceKey4", reduceKeyAndTypes[0].ReduceKey);
				});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void GetKeyStats(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor => Assert.Equal(0, accessor.MapReduce.GetKeysStats(303, 0, 10).Count()));

				storage.Batch(accessor => accessor.MapReduce.IncrementReduceKeyCounter(303, "reduceKey1", 7));
				storage.Batch(accessor => Assert.Equal(1, accessor.MapReduce.GetKeysStats(303, 0, 10).Count()));

				storage.Batch(accessor => accessor.MapReduce.IncrementReduceKeyCounter(303, "reduceKey1", 7));
				storage.Batch(accessor => Assert.Equal(1, accessor.MapReduce.GetKeysStats(303, 0, 10).Count()));

				storage.Batch(accessor => accessor.MapReduce.IncrementReduceKeyCounter(303, "reduceKey2", 7));
				storage.Batch(accessor => accessor.MapReduce.IncrementReduceKeyCounter(303, "reduceKey3", 7));
				storage.Batch(accessor => accessor.MapReduce.IncrementReduceKeyCounter(404, "reduceKey1", 7));
				storage.Batch(accessor => Assert.Equal(3, accessor.MapReduce.GetKeysStats(303, 0, 10).Count()));

				storage.Batch(accessor =>
				{
					var keyStats = accessor.MapReduce.GetKeysStats(303, 0, 1).ToList();
					Assert.Equal(1, keyStats.Count);
					var k1 = keyStats[0];

					keyStats = accessor.MapReduce.GetKeysStats(303, 1, 1).ToList();
					Assert.Equal(1, keyStats.Count);
					var k2 = keyStats[0];

					keyStats = accessor.MapReduce.GetKeysStats(303, 2, 1).ToList();
					Assert.Equal(1, keyStats.Count);
					var k3 = keyStats[0];

					Assert.NotEqual(k1.Key, k2.Key);
					Assert.NotEqual(k1.Key, k3.Key);
					Assert.NotEqual(k2.Key, k3.Key);

					keyStats = accessor.MapReduce.GetKeysStats(303, 1, 2).ToList();
					Assert.Equal(2, keyStats.Count);
					Assert.Equal(k2.Key, keyStats[0].Key);
					Assert.Equal(k3.Key, keyStats[1].Key);

					keyStats = accessor.MapReduce.GetKeysStats(303, 2, 2).ToList();
					Assert.Equal(1, keyStats.Count);
					Assert.Equal(k3.Key, keyStats[0].Key);

					keyStats = accessor.MapReduce.GetKeysStats(303, 0, 5).ToList();
					Assert.Equal(3, keyStats.Count);
					Assert.Equal(k1.Key, keyStats[0].Key);
					Assert.Equal(k2.Key, keyStats[1].Key);
					Assert.Equal(k3.Key, keyStats[2].Key);

					keyStats = accessor.MapReduce.GetKeysStats(303, 5, 55).ToList();
					Assert.Equal(0, keyStats.Count);

					keyStats = accessor.MapReduce.GetKeysStats(404, 0, 10).ToList();
					Assert.Equal(1, keyStats.Count);
					Assert.Equal("reduceKey1", keyStats[0].Key);
				});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void UpdatePerformedReduceType(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor => accessor.MapReduce.UpdatePerformedReduceType(303, "reduceKey1", ReduceType.None));

				storage.Batch(
					accessor =>
					{
						var keyStats = accessor.MapReduce.GetKeysStats(303, 0, 10).ToList();
						Assert.Equal(0, keyStats.Count);

						var reduceKeysAndTypes = accessor.MapReduce.GetReduceKeysAndTypes(303, 0, 10).ToList();
						Assert.Equal(1, reduceKeysAndTypes.Count);
						Assert.Equal("reduceKey1", reduceKeysAndTypes[0].ReduceKey);
						Assert.Equal(ReduceType.None, reduceKeysAndTypes[0].OperationTypeToPerform);
					});

				storage.Batch(accessor => accessor.MapReduce.UpdatePerformedReduceType(303, "reduceKey1", ReduceType.SingleStep));

				storage.Batch(
					accessor =>
					{
						var keyStats = accessor.MapReduce.GetKeysStats(303, 0, 10).ToList();
						Assert.Equal(0, keyStats.Count);

						var reduceKeysAndTypes = accessor.MapReduce.GetReduceKeysAndTypes(303, 0, 10).ToList();
						Assert.Equal(1, reduceKeysAndTypes.Count);
						Assert.Equal("reduceKey1", reduceKeysAndTypes[0].ReduceKey);
						Assert.Equal(ReduceType.SingleStep, reduceKeysAndTypes[0].OperationTypeToPerform);
					});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void IncrementReduceKeyCounterDoesNotInterfereWithUpdatePerformedReduceType(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor => accessor.MapReduce.UpdatePerformedReduceType(303, "reduceKey1", ReduceType.MultiStep));
				storage.Batch(accessor => accessor.MapReduce.IncrementReduceKeyCounter(303, "reduceKey1", 5));

				storage.Batch(
					accessor =>
					{
						var reduceKeysAndTypes = accessor.MapReduce.GetReduceKeysAndTypes(303, 0, 10).ToList();
						var keyStats = accessor.MapReduce.GetKeysStats(303, 0, 10).ToList();

						Assert.Equal(1, reduceKeysAndTypes.Count);
						Assert.Equal("reduceKey1", reduceKeysAndTypes[0].ReduceKey);
						Assert.Equal(ReduceType.MultiStep, reduceKeysAndTypes[0].OperationTypeToPerform);

						Assert.Equal(1, keyStats.Count);
						Assert.Equal("reduceKey1", keyStats[0].Key);
						Assert.Equal(5, keyStats[0].Count);
					});

				storage.Batch(accessor => accessor.MapReduce.IncrementReduceKeyCounter(404, "reduceKey2", 5));
				storage.Batch(accessor => accessor.MapReduce.UpdatePerformedReduceType(404, "reduceKey2", ReduceType.MultiStep));

				storage.Batch(
					accessor =>
					{
						var reduceKeysAndTypes = accessor.MapReduce.GetReduceKeysAndTypes(404, 0, 10).ToList();
						var keyStats = accessor.MapReduce.GetKeysStats(404, 0, 10).ToList();

						Assert.Equal(1, reduceKeysAndTypes.Count);
						Assert.Equal("reduceKey2", reduceKeysAndTypes[0].ReduceKey);
						Assert.Equal(ReduceType.MultiStep, reduceKeysAndTypes[0].OperationTypeToPerform);

						Assert.Equal(1, keyStats.Count);
						Assert.Equal("reduceKey2", keyStats[0].Key);
						Assert.Equal(5, keyStats[0].Count);
					});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void PutMappedResult(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(x => x.MapReduce.PutMappedResult(303, "doc1", "reduceKey1", new RavenJObject { { "data", "data1" } }));

				storage.Batch(x =>
				{
					var results = x.MapReduce
						.GetMappedResults(303, new List<string> { "reduceKey1" }, true)
						.ToList();

					Assert.Equal(1, results.Count);

					var result = results[0];
					Assert.NotEqual(Etag.InvalidEtag, result.Etag);
					Assert.Equal("reduceKey1", result.ReduceKey);
					Assert.True(result.Size > 0);
					Assert.Null(result.Source);
					Assert.True((DateTime.UtcNow - result.Timestamp).TotalMilliseconds < 100);
					Assert.Equal("data1", result.Data["data"]);
				});

				storage.Batch(x => x.MapReduce.PutMappedResult(303, "doc1", "reduceKey1", new RavenJObject { { "data", "data2" } }));

				storage.Batch(x =>
				{
					var results = x.MapReduce
						.GetMappedResults(303, new List<string> { "reduceKey1" }, true)
						.ToList();

					Assert.Equal(2, results.Count);

					var result1 = results[0];
					Assert.NotEqual(Etag.InvalidEtag, result1.Etag);
					Assert.Equal("reduceKey1", result1.ReduceKey);
					Assert.True(result1.Size > 0);
					Assert.Null(result1.Source);
					Assert.True((DateTime.UtcNow - result1.Timestamp).TotalMilliseconds < 100);
					Assert.Equal("data1", result1.Data["data"]);

					var result2 = results[1];
					Assert.NotEqual(Etag.InvalidEtag, result2.Etag);
					Assert.Equal("reduceKey1", result2.ReduceKey);
					Assert.True(result2.Size > 0);
					Assert.Null(result2.Source);
					Assert.True((DateTime.UtcNow - result2.Timestamp).TotalMilliseconds < 100);
					Assert.Equal("data2", result2.Data["data"]);
				});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void DeleteMappedResultsForDocumentId(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(x =>
				{
					var removed = new Dictionary<ReduceKeyAndBucket, int>();
					x.MapReduce.DeleteMappedResultsForDocumentId("doc1", 303, removed);

					Assert.Equal(0, removed.Count);
				});

				storage.Batch(x => x.MapReduce.PutMappedResult(303, "doc1", "reduceKey1", new RavenJObject { { "data", "data1" } }));
				storage.Batch(
					x =>
					{
						var results = x.MapReduce
							.GetMappedResults(303, new List<string> { "reduceKey1" }, true)
							.ToList();

						Assert.Equal(1, results.Count);
					});

				storage.Batch(x =>
				{
					var removed = new Dictionary<ReduceKeyAndBucket, int>();
					x.MapReduce.DeleteMappedResultsForDocumentId("doc1", 303, removed);

					Assert.Equal(1, removed.Count);
				});

				storage.Batch(
					x =>
					{
						var results = x.MapReduce
							.GetMappedResults(303, new List<string> { "reduceKey1" }, true)
							.ToList();

						Assert.Equal(0, results.Count);
					});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void DeleteMappedResultsForDocumentIdMultipleMappedResults(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(x => x.MapReduce.PutMappedResult(303, "doc1", "reduceKey1", new RavenJObject { { "data", "data1" } }));
				storage.Batch(x => x.MapReduce.PutMappedResult(303, "doc1", "reduceKey1", new RavenJObject { { "data", "data2" } }));
				storage.Batch(x => x.MapReduce.PutMappedResult(303, "doc2", "reduceKey1", new RavenJObject { { "data", "data3" } }));
				storage.Batch(
					x =>
					{
						var results = x.MapReduce
							.GetMappedResults(303, new List<string> { "reduceKey1", "reduceKey2" }, true)
							.ToList();

						Assert.Equal(3, results.Count);
					});

				storage.Batch(x =>
				{
					var removed = new Dictionary<ReduceKeyAndBucket, int>();
					x.MapReduce.DeleteMappedResultsForDocumentId("doc1", 303, removed);

					Assert.Equal(1, removed.Count);
					var item = removed.First();

					Assert.Equal("reduceKey1", item.Key.ReduceKey);
					Assert.Equal(2, item.Value);
				});

				storage.Batch(
					x =>
					{
						var results = x.MapReduce
							.GetMappedResults(303, new List<string> { "reduceKey1", "reduceKey2" }, true)
							.ToList();

						Assert.Equal(1, results.Count);
					});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void UpdateRemovedMapReduceStats(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor => accessor.MapReduce.IncrementReduceKeyCounter(303, "reduceKey1", 7));
				storage.Batch(accessor => accessor.MapReduce.IncrementReduceKeyCounter(404, "reduceKey1", 3));

				storage.Batch(accessor =>
				{
					var removed = new Dictionary<ReduceKeyAndBucket, int>();
					accessor.MapReduce.UpdateRemovedMapReduceStats(303, removed);
				});

				storage.Batch(accessor =>
				{
					var reduceKeysAndTypes = accessor.MapReduce.GetReduceKeysAndTypes(303, 0, 10).ToList();
					Assert.Equal(0, reduceKeysAndTypes.Count);

					var keyStats = accessor.MapReduce.GetKeysStats(303, 0, 10).ToList();
					Assert.Equal(1, keyStats.Count);

					var k1 = keyStats[0];
					Assert.Equal("reduceKey1", k1.Key);
					Assert.Equal(7, k1.Count);
				});

				storage.Batch(accessor =>
				{
					var removed = new Dictionary<ReduceKeyAndBucket, int>
					              {
						              { new ReduceKeyAndBucket(123, "reduceKey1"), 3 }
					              };

					accessor.MapReduce.UpdateRemovedMapReduceStats(303, removed);
				});

				storage.Batch(accessor =>
				{
					var reduceKeysAndTypes = accessor.MapReduce.GetReduceKeysAndTypes(303, 0, 10).ToList();
					Assert.Equal(0, reduceKeysAndTypes.Count);

					var keyStats = accessor.MapReduce.GetKeysStats(303, 0, 10).ToList();
					Assert.Equal(1, keyStats.Count);

					var k1 = keyStats[0];
					Assert.Equal("reduceKey1", k1.Key);
					Assert.Equal(4, k1.Count);
				});

				storage.Batch(accessor =>
				{
					var removed = new Dictionary<ReduceKeyAndBucket, int>
					              {
						              { new ReduceKeyAndBucket(123, "reduceKey1"), 4 }
					              };

					accessor.MapReduce.UpdateRemovedMapReduceStats(303, removed);
					accessor.MapReduce.UpdateRemovedMapReduceStats(404, removed);
				});

				storage.Batch(accessor =>
				{
					var reduceKeysAndTypes = accessor.MapReduce.GetReduceKeysAndTypes(303, 0, 10).ToList();
					Assert.Equal(0, reduceKeysAndTypes.Count);

					var keyStats = accessor.MapReduce.GetKeysStats(303, 0, 10).ToList();
					Assert.Equal(0, keyStats.Count);

					reduceKeysAndTypes = accessor.MapReduce.GetReduceKeysAndTypes(404, 0, 10).ToList();
					Assert.Equal(0, reduceKeysAndTypes.Count);

					keyStats = accessor.MapReduce.GetKeysStats(404, 0, 10).ToList();
					Assert.Equal(1, keyStats.Count);

					var k1 = keyStats[0];
					Assert.Equal("reduceKey1", k1.Key);
					Assert.Equal(-1, k1.Count);
				});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void DeleteMappedResultsForView(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor => accessor.MapReduce.DeleteMappedResultsForView(303));

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce
						.GetMappedResults(303, new List<string> { "reduceKey1", "reduceKey2" }, true)
						.ToList();

					Assert.Equal(0, results.Count);

					results = accessor.MapReduce
					   .GetMappedResults(404, new List<string> { "reduceKey1", "reduceKey2" }, true)
					   .ToList();

					Assert.Equal(0, results.Count);
				});

				storage.Batch(accessor =>
				{
					accessor.MapReduce.PutMappedResult(303, "doc1", "reduceKey1", new RavenJObject { { "data", "data1" } });
					accessor.MapReduce.PutMappedResult(303, "doc2", "reduceKey1", new RavenJObject { { "data", "data2" } });
					accessor.MapReduce.PutMappedResult(303, "doc1", "reduceKey2", new RavenJObject { { "data", "data3" } });
					accessor.MapReduce.PutMappedResult(404, "doc1", "reduceKey1", new RavenJObject { { "data", "data4" } });
					accessor.MapReduce.IncrementReduceKeyCounter(303, "reduceKey1", 2);
					accessor.MapReduce.IncrementReduceKeyCounter(303, "reduceKey2", 1);
					accessor.MapReduce.IncrementReduceKeyCounter(404, "reduceKey1", 1);
				});

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce
						.GetMappedResults(303, new List<string> { "reduceKey1", "reduceKey2" }, true)
						.ToList();

					Assert.Equal(3, results.Count);

					var reduceKeysAndTypes = accessor.MapReduce.GetReduceKeysAndTypes(303, 0, 10).ToList();
					Assert.Equal(0, reduceKeysAndTypes.Count);

					var keyStats = accessor.MapReduce.GetKeysStats(303, 0, 10).ToList();
					Assert.Equal(2, keyStats.Count);

					var k1 = keyStats[0];
					Assert.Equal("reduceKey1", k1.Key);
					Assert.Equal(2, k1.Count);

					var k2 = keyStats[1];
					Assert.Equal("reduceKey2", k2.Key);
					Assert.Equal(1, k2.Count);

					results = accessor.MapReduce
					   .GetMappedResults(404, new List<string> { "reduceKey1", "reduceKey2" }, true)
					   .ToList();

					Assert.Equal(1, results.Count);

					reduceKeysAndTypes = accessor.MapReduce.GetReduceKeysAndTypes(404, 0, 10).ToList();
					Assert.Equal(0, reduceKeysAndTypes.Count);

					keyStats = accessor.MapReduce.GetKeysStats(404, 0, 10).ToList();
					Assert.Equal(1, keyStats.Count);

					k1 = keyStats[0];
					Assert.Equal("reduceKey1", k1.Key);
					Assert.Equal(1, k1.Count);
				});

				storage.Batch(accessor => accessor.MapReduce.DeleteMappedResultsForView(303));

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce
						.GetMappedResults(303, new List<string> { "reduceKey1", "reduceKey2" }, true)
						.ToList();

					Assert.Equal(0, results.Count);

					var reduceKeysAndTypes = accessor.MapReduce.GetReduceKeysAndTypes(303, 0, 10).ToList();
					Assert.Equal(0, reduceKeysAndTypes.Count);

					var keyStats = accessor.MapReduce.GetKeysStats(303, 0, 10).ToList();
					Assert.Equal(0, keyStats.Count);

					results = accessor.MapReduce
					   .GetMappedResults(404, new List<string> { "reduceKey1", "reduceKey2" }, true)
					   .ToList();

					Assert.Equal(1, results.Count);

					reduceKeysAndTypes = accessor.MapReduce.GetReduceKeysAndTypes(404, 0, 10).ToList();
					Assert.Equal(0, reduceKeysAndTypes.Count);

					keyStats = accessor.MapReduce.GetKeysStats(404, 0, 10).ToList();
					Assert.Equal(1, keyStats.Count);

					var k1 = keyStats[0];
					Assert.Equal("reduceKey1", k1.Key);
					Assert.Equal(1, k1.Count);
				});

				storage.Batch(accessor => accessor.MapReduce.DeleteMappedResultsForView(404));

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce
						.GetMappedResults(303, new List<string> { "reduceKey1", "reduceKey2" }, true)
						.ToList();

					Assert.Equal(0, results.Count);

					var reduceKeysAndTypes = accessor.MapReduce.GetReduceKeysAndTypes(303, 0, 10).ToList();
					Assert.Equal(0, reduceKeysAndTypes.Count);

					var keyStats = accessor.MapReduce.GetKeysStats(303, 0, 10).ToList();
					Assert.Equal(0, keyStats.Count);

					results = accessor.MapReduce
					   .GetMappedResults(404, new List<string> { "reduceKey1", "reduceKey2" }, true)
					   .ToList();

					Assert.Equal(0, results.Count);

					reduceKeysAndTypes = accessor.MapReduce.GetReduceKeysAndTypes(404, 0, 10).ToList();
					Assert.Equal(0, reduceKeysAndTypes.Count);

					keyStats = accessor.MapReduce.GetKeysStats(404, 0, 10).ToList();
					Assert.Equal(0, keyStats.Count);
				});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void GetKeysForIndexForDebug(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor =>
				{
					accessor.MapReduce.PutMappedResult(303, "doc1", "reduceKey1", new RavenJObject { { "data", "data1" } });
					accessor.MapReduce.PutMappedResult(303, "doc2", "reduceKey2", new RavenJObject { { "data", "data2" } });
					accessor.MapReduce.PutMappedResult(303, "doc3", "reduceKey3", new RavenJObject { { "data", "data3" } });
					accessor.MapReduce.PutMappedResult(404, "doc1", "reduceKey4", new RavenJObject { { "data", "data4" } });
				});

				storage.Batch(accessor =>
				{
					var keys = accessor.MapReduce
						.GetKeysForIndexForDebug(303, 0, 10)
						.ToList();

					Assert.Equal(3, keys.Count);
					Assert.True(keys.Contains("reduceKey1"));
					Assert.True(keys.Contains("reduceKey2"));
					Assert.True(keys.Contains("reduceKey3"));

					keys = accessor.MapReduce
						.GetKeysForIndexForDebug(404, 0, 10)
						.ToList();

					Assert.Equal(1, keys.Count);
					Assert.Equal("reduceKey4", keys[0]);

					keys = accessor.MapReduce
						.GetKeysForIndexForDebug(505, 0, 10)
						.ToList();

					Assert.Equal(0, keys.Count);
				});

				storage.Batch(accessor =>
				{
					var keys = accessor.MapReduce
						.GetKeysForIndexForDebug(303, 0, 10)
						.ToList();

					var k1 = keys[0];
					var k2 = keys[1];
					var k3 = keys[2];

					keys = accessor.MapReduce
						.GetKeysForIndexForDebug(303, 0, 1)
						.ToList();

					Assert.Equal(1, keys.Count);
					Assert.Equal(k1, keys[0]);

					keys = accessor.MapReduce
						.GetKeysForIndexForDebug(303, 1, 1)
						.ToList();

					Assert.Equal(1, keys.Count);
					Assert.Equal(k2, keys[0]);

					keys = accessor.MapReduce
						.GetKeysForIndexForDebug(303, 2, 1)
						.ToList();

					Assert.Equal(1, keys.Count);
					Assert.Equal(k3, keys[0]);

					keys = accessor.MapReduce
						.GetKeysForIndexForDebug(303, 3, 1)
						.ToList();

					Assert.Equal(0, keys.Count);

					keys = accessor.MapReduce
						.GetKeysForIndexForDebug(303, 0, 2)
						.ToList();

					Assert.Equal(2, keys.Count);
					Assert.Equal(k1, keys[0]);
					Assert.Equal(k2, keys[1]);

					keys = accessor.MapReduce
						.GetKeysForIndexForDebug(303, 1, 2)
						.ToList();

					Assert.Equal(2, keys.Count);
					Assert.Equal(k2, keys[0]);
					Assert.Equal(k3, keys[1]);

					keys = accessor.MapReduce
						.GetKeysForIndexForDebug(303, 2, 2)
						.ToList();

					Assert.Equal(1, keys.Count);
					Assert.Equal(k3, keys[0]);
				});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void GetMappedResultsForDebug(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor =>
				{
					accessor.MapReduce.PutMappedResult(303, "doc1", "reduceKey1", new RavenJObject { { "data", "data1" } });
					accessor.MapReduce.PutMappedResult(303, "doc2", "reduceKey2", new RavenJObject { { "data", "data2" } });
					accessor.MapReduce.PutMappedResult(303, "doc3", "reduceKey1", new RavenJObject { { "data", "data3" } });
					accessor.MapReduce.PutMappedResult(303, "doc4", "reduceKey1", new RavenJObject { { "data", "data4" } });
					accessor.MapReduce.PutMappedResult(404, "doc1", "reduceKey4", new RavenJObject { { "data", "data5" } });
				});

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce
						.GetMappedResultsForDebug(303, "reduceKey1", 0, 10)
						.ToList();

					Assert.Equal(3, results.Count);

					results = accessor.MapReduce
						.GetMappedResultsForDebug(303, "reduceKey2", 0, 10)
						.ToList();

					Assert.Equal(1, results.Count);

					results = accessor.MapReduce
						.GetMappedResultsForDebug(404, "reduceKey4", 0, 10)
						.ToList();

					Assert.Equal(1, results.Count);

					results = accessor.MapReduce
						.GetMappedResultsForDebug(505, "reduceKey1", 0, 10)
						.ToList();

					Assert.Equal(0, results.Count);
				});

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce
						.GetMappedResultsForDebug(303, "reduceKey1", 0, 10)
						.ToList();

					var r1 = results[0];
					var r2 = results[1];
					var r3 = results[2];

					results = accessor.MapReduce
						.GetMappedResultsForDebug(303, "reduceKey1", 0, 1)
						.ToList();

					Assert.Equal(1, results.Count);
					Assert.Equal(r1.Data["data"], results[0].Data["data"]);

					results = accessor.MapReduce
						.GetMappedResultsForDebug(303, "reduceKey1", 1, 1)
						.ToList();

					Assert.Equal(1, results.Count);
					Assert.Equal(r2.Data["data"], results[0].Data["data"]);

					results = accessor.MapReduce
						.GetMappedResultsForDebug(303, "reduceKey1", 2, 1)
						.ToList();

					Assert.Equal(1, results.Count);
					Assert.Equal(r3.Data["data"], results[0].Data["data"]);

					results = accessor.MapReduce
						.GetMappedResultsForDebug(303, "reduceKey1", 4, 1)
						.ToList();

					Assert.Equal(0, results.Count);

					results = accessor.MapReduce
						.GetMappedResultsForDebug(303, "reduceKey1", 0, 2)
						.ToList();

					Assert.Equal(2, results.Count);
					Assert.Equal(r1.Data["data"], results[0].Data["data"]);
					Assert.Equal(r2.Data["data"], results[1].Data["data"]);

					results = accessor.MapReduce
						.GetMappedResultsForDebug(303, "reduceKey1", 1, 2)
						.ToList();

					Assert.Equal(2, results.Count);
					Assert.Equal(r2.Data["data"], results[0].Data["data"]);
					Assert.Equal(r3.Data["data"], results[1].Data["data"]);

					results = accessor.MapReduce
						.GetMappedResultsForDebug(303, "reduceKey1", 2, 2)
						.ToList();

					Assert.Equal(1, results.Count);
					Assert.Equal(r3.Data["data"], results[0].Data["data"]);
				});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void GetReducedResultsForDebug(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor =>
				{
					accessor.MapReduce.PutReducedResult(303, "doc1", 1, 1, 1, new RavenJObject { { "data", "data1" } });
					accessor.MapReduce.PutReducedResult(303, "doc2", 1, 1, 1, new RavenJObject { { "data", "data2" } });
					accessor.MapReduce.PutReducedResult(303, "doc1", 2, 2, 1, new RavenJObject { { "data", "data3" } });
					accessor.MapReduce.PutReducedResult(303, "doc1", 1, 1, 1, new RavenJObject { { "data", "data4" } });
					accessor.MapReduce.PutReducedResult(303, "doc1", 1, 2, 1, new RavenJObject { { "data", "data5" } });
					accessor.MapReduce.PutReducedResult(303, "doc1", 2, 1, 1, new RavenJObject { { "data", "data6" } });
					accessor.MapReduce.PutReducedResult(404, "doc1", 1, 1, 1, new RavenJObject { { "data", "data7" } });
				});

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce
						.GetReducedResultsForDebug(303, "doc1", 1, 0, 10)
						.ToList();

					Assert.Equal(3, results.Count);

					results = accessor.MapReduce
						.GetReducedResultsForDebug(303, "doc2", 1, 0, 10)
						.ToList();

					Assert.Equal(1, results.Count);

					results = accessor.MapReduce
						.GetReducedResultsForDebug(303, "doc1", 2, 0, 10)
						.ToList();

					Assert.Equal(2, results.Count);

					results = accessor.MapReduce
						.GetReducedResultsForDebug(404, "doc1", 1, 0, 10)
						.ToList();

					Assert.Equal(1, results.Count);

					results = accessor.MapReduce
						.GetReducedResultsForDebug(303, "doc5", 1, 0, 10)
						.ToList();

					Assert.Equal(0, results.Count);
				});

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce
						.GetReducedResultsForDebug(303, "doc1", 1, 0, 10)
						.ToList();

					var r1 = results[0];
					var r2 = results[1];
					var r3 = results[2];

					results = accessor.MapReduce
						.GetReducedResultsForDebug(303, "doc1", 1, 0, 1)
						.ToList();

					Assert.Equal(1, results.Count);
					Assert.Equal(r1.Data["data"], results[0].Data["data"]);

					results = accessor.MapReduce
						.GetReducedResultsForDebug(303, "doc1", 1, 1, 1)
						.ToList();

					Assert.Equal(1, results.Count);
					Assert.Equal(r2.Data["data"], results[0].Data["data"]);

					results = accessor.MapReduce
						.GetReducedResultsForDebug(303, "doc1", 1, 2, 1)
						.ToList();

					Assert.Equal(1, results.Count);
					Assert.Equal(r3.Data["data"], results[0].Data["data"]);

					results = accessor.MapReduce
						.GetReducedResultsForDebug(303, "doc1", 1, 4, 1)
						.ToList();

					Assert.Equal(0, results.Count);

					results = accessor.MapReduce
						.GetReducedResultsForDebug(303, "doc1", 1, 0, 2)
						.ToList();

					Assert.Equal(2, results.Count);
					Assert.Equal(r1.Data["data"], results[0].Data["data"]);
					Assert.Equal(r2.Data["data"], results[1].Data["data"]);

					results = accessor.MapReduce
						.GetReducedResultsForDebug(303, "doc1", 1, 1, 2)
						.ToList();

					Assert.Equal(2, results.Count);
					Assert.Equal(r2.Data["data"], results[0].Data["data"]);
					Assert.Equal(r3.Data["data"], results[1].Data["data"]);

					results = accessor.MapReduce
						.GetReducedResultsForDebug(303, "doc1", 1, 2, 2)
						.ToList();

					Assert.Equal(1, results.Count);
					Assert.Equal(r3.Data["data"], results[0].Data["data"]);
				});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void GetScheduledReductionForDebug(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor =>
				{
					accessor.MapReduce.ScheduleReductions(303, 1, new ReduceKeyAndBucket(1, "reduceKey1"));
					accessor.MapReduce.ScheduleReductions(303, 2, new ReduceKeyAndBucket(1, "reduceKey2"));
					accessor.MapReduce.ScheduleReductions(303, 3, new ReduceKeyAndBucket(2, "reduceKey2"));
					accessor.MapReduce.ScheduleReductions(404, 1, new ReduceKeyAndBucket(1, "reduceKey3"));
				});

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce
						.GetScheduledReductionForDebug(303, 0, 10)
						.ToList();

					Assert.Equal(3, results.Count);

					var r1 = results[0];
					var r2 = results[1];
					var r3 = results[2];

					results = accessor.MapReduce
						.GetScheduledReductionForDebug(303, 0, 1)
						.ToList();

					Assert.Equal(1, results.Count);
					Assert.Equal(r1.Key, results[0].Key);

					results = accessor.MapReduce
						.GetScheduledReductionForDebug(303, 1, 1)
						.ToList();

					Assert.Equal(1, results.Count);
					Assert.Equal(r2.Key, results[0].Key);

					results = accessor.MapReduce
						.GetScheduledReductionForDebug(303, 2, 1)
						.ToList();

					Assert.Equal(1, results.Count);
					Assert.Equal(r3.Key, results[0].Key);

					results = accessor.MapReduce
						.GetScheduledReductionForDebug(303, 3, 1)
						.ToList();

					Assert.Equal(0, results.Count);

					results = accessor.MapReduce
						.GetScheduledReductionForDebug(303, 0, 2)
						.ToList();

					Assert.Equal(2, results.Count);
					Assert.Equal(r1.Key, results[0].Key);
					Assert.Equal(r2.Key, results[1].Key);

					results = accessor.MapReduce
						.GetScheduledReductionForDebug(303, 1, 2)
						.ToList();

					Assert.Equal(2, results.Count);
					Assert.Equal(r2.Key, results[0].Key);
					Assert.Equal(r3.Key, results[1].Key);

					results = accessor.MapReduce
						.GetScheduledReductionForDebug(303, 2, 2)
						.ToList();

					Assert.Equal(1, results.Count);
					Assert.Equal(r3.Key, results[0].Key);

					results = accessor.MapReduce
						.GetScheduledReductionForDebug(404, 0, 10)
						.ToList();

					Assert.Equal(1, results.Count);
					Assert.Equal("reduceKey3", results[0].Key);

					results = accessor.MapReduce
						.GetScheduledReductionForDebug(505, 0, 10)
						.ToList();

					Assert.Equal(0, results.Count);
				});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void ScheduleReductions(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor => accessor.MapReduce.ScheduleReductions(303, 1, new ReduceKeyAndBucket(1, "reduceKey1")));

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce
						.GetScheduledReductionForDebug(303, 0, 10)
						.ToList();

					Assert.Equal(1, results.Count);

					var r1 = results[0];

					Assert.Equal(1, r1.Level);
					Assert.Equal("reduceKey1", r1.Key);
					Assert.True((SystemTime.UtcNow - r1.Timestamp).TotalMilliseconds < 100);
				});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void GetItemsToReduce(string requestedStorage)
		{
			int bucket1 = IndexingUtil.MapBucket("doc1");
			int bucket2 = IndexingUtil.MapBucket("doc2");

			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor => Assert.Equal(0, accessor.MapReduce
					.GetItemsToReduce(new GetItemsToReduceParams(303, new List<string>(), 0, true, new List<object>()))
					.Count()));

				storage.Batch(accessor =>
				{
					accessor.MapReduce.ScheduleReductions(303, 0, new ReduceKeyAndBucket(bucket1, "reduceKey1"));
					accessor.MapReduce.ScheduleReductions(303, 1, new ReduceKeyAndBucket(bucket2, "reduceKey1"));
					accessor.MapReduce.ScheduleReductions(303, 0, new ReduceKeyAndBucket(bucket1, "reduceKey2"));
					accessor.MapReduce.ScheduleReductions(303, 1, new ReduceKeyAndBucket(bucket2, "reduceKey2"));
				});

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce
						.GetItemsToReduce(new GetItemsToReduceParams(303, new List<string> { "reduceKey1" }, 1, false, new List<object>()))
						.ToList();

					Assert.Equal(0, results.Count);

					results = accessor.MapReduce
						.GetItemsToReduce(new GetItemsToReduceParams(303, new List<string> { "reduceKey1" }, 1, false, new List<object>())
										  {
											  Take = 10
										  })
						.ToList();

					Assert.Equal(1, results.Count);
					Assert.Equal("reduceKey1", results[0].ReduceKey);
					Assert.Equal(bucket1, results[0].Bucket);

					results = accessor.MapReduce
						.GetItemsToReduce(new GetItemsToReduceParams(303, new List<string> { "reduceKey1" }, 0, false, new List<object>()))
						.ToList();

					Assert.Equal(0, results.Count);

					results = accessor.MapReduce
						.GetItemsToReduce(new GetItemsToReduceParams(303, new List<string> { "reduceKey1" }, 0, false, new List<object>())
						{
							Take = 10
						})
						.ToList();

					Assert.Equal(1, results.Count);
					Assert.Equal("reduceKey1", results[0].ReduceKey);
					Assert.Equal(bucket2, results[0].Bucket);
				});

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce
						.GetItemsToReduce(new GetItemsToReduceParams(303, new List<string> { "reduceKey1", "reduceKey2" }, 1, false, new List<object>()))
						.ToList();

					Assert.Equal(0, results.Count);

					results = accessor.MapReduce
						.GetItemsToReduce(new GetItemsToReduceParams(303, new List<string> { "reduceKey1", "reduceKey2" }, 1, false, new List<object>())
						{
							Take = 10
						})
						.ToList();

					Assert.Equal(2, results.Count);

					results = accessor.MapReduce
						.GetItemsToReduce(new GetItemsToReduceParams(303, new List<string> { "reduceKey1", "reduceKey2" }, 0, false, new List<object>()))
						.ToList();

					Assert.Equal(0, results.Count);

					results = accessor.MapReduce
						.GetItemsToReduce(new GetItemsToReduceParams(303, new List<string> { "reduceKey1", "reduceKey2" }, 0, false, new List<object>())
						{
							Take = 10
						})
						.ToList();

					Assert.Equal(2, results.Count);
				});

				storage.Batch(accessor =>
				{
					accessor.MapReduce.PutReducedResult(303, "reduceKey1", 1, bucket1, bucket2, new RavenJObject { { "data", "data1" } });
					accessor.MapReduce.PutReducedResult(303, "reduceKey1", 1, bucket1, bucket2, new RavenJObject { { "data", "data2" } });
					accessor.MapReduce.PutMappedResult(303, "doc1", "reduceKey1", new RavenJObject { { "data", "data3" } });
					accessor.MapReduce.PutMappedResult(303, "doc1", "reduceKey1", new RavenJObject { { "data", "data4" } });
				});

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce
						.GetItemsToReduce(new GetItemsToReduceParams(303, new List<string> { "reduceKey1" }, 0, true, new List<object>())
						{
							Take = 10
						})
						.ToList();

					Assert.Equal(2, results.Count);
					Assert.True(results.Any(x => x.Source == null && x.Data["data"].ToString() == "data3"));
					Assert.True(results.Any(x => x.Source == null && x.Data["data"].ToString() == "data4"));

					results = accessor.MapReduce
						.GetItemsToReduce(new GetItemsToReduceParams(303, new List<string> { "reduceKey1" }, 1, true, new List<object>())
						{
							Take = 10
						})
						.ToList();

					Assert.Equal(2, results.Count);
					Assert.True(results.Any(x => x.Source == null && x.Data["data"].ToString() == "data1"));
					Assert.True(results.Any(x => x.Source == null && x.Data["data"].ToString() == "data2"));
				});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void DeleteScheduledReduction1(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				ScheduledReductionDebugInfo r1 = null;
				ScheduledReductionDebugInfo r2 = null;
				ScheduledReductionDebugInfo r3 = null;

				storage.Batch(accessor => accessor.MapReduce.DeleteScheduledReduction(303, 1, "reduceKey1"));

				storage.Batch(accessor =>
				{
					accessor.MapReduce.ScheduleReductions(303, 1, new ReduceKeyAndBucket(1, "reduceKey1"));
					accessor.MapReduce.ScheduleReductions(303, 2, new ReduceKeyAndBucket(1, "reduceKey2"));
					accessor.MapReduce.ScheduleReductions(303, 3, new ReduceKeyAndBucket(2, "reduceKey2"));
					accessor.MapReduce.ScheduleReductions(404, 1, new ReduceKeyAndBucket(1, "reduceKey3"));
				});

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce
						.GetScheduledReductionForDebug(303, 0, 10)
						.ToList();

					Assert.Equal(3, results.Count);

					r1 = results[0];
					r2 = results[1];
					r3 = results[2];
				});

				storage.Batch(accessor => accessor.MapReduce.DeleteScheduledReduction(303, 3, "reduceKey3"));
				storage.Batch(accessor => Assert.Equal(3, accessor.MapReduce.GetScheduledReductionForDebug(303, 0, 10).Count()));

				storage.Batch(accessor => accessor.MapReduce.DeleteScheduledReduction(303, 3, "reduceKey2"));
				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce
						.GetScheduledReductionForDebug(303, 0, 10)
						.ToList();

					Assert.Equal(2, results.Count);
					Assert.Equal(r1.Key, results[0].Key);
					Assert.Equal(r2.Key, results[1].Key);

					results = accessor.MapReduce
						.GetScheduledReductionForDebug(404, 0, 10)
						.ToList();

					Assert.Equal(1, results.Count);
					Assert.Equal("reduceKey3", results[0].Key);
				});

				storage.Batch(accessor => accessor.MapReduce.DeleteScheduledReduction(404, 1, "reduceKey3"));
				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce
						.GetScheduledReductionForDebug(404, 0, 10)
						.ToList();

					Assert.Equal(0, results.Count);
				});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void PutReducedResult(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor => accessor.MapReduce.PutReducedResult(303, "reduceKey1", 1, 2, 3, new RavenJObject { { "data", "data1" } }));

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce
						.GetReducedResultsForDebug(303, "reduceKey1", 1, 0, 10)
						.ToList();

					Assert.Equal(1, results.Count);

					var r1 = results[0];

					Assert.NotEqual(Etag.InvalidEtag, r1.Etag);
					Assert.Equal("reduceKey1", r1.ReduceKey);
					Assert.True(r1.Size > 0);
					Assert.Equal("2", r1.Source);
					Assert.True((DateTime.UtcNow - r1.Timestamp).TotalMilliseconds < 100);
					Assert.Equal("data1", r1.Data["data"]);
				});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void RemoveReducedResult(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor => accessor.MapReduce.PutReducedResult(303, "reduceKey1", 1, 2, 3, new RavenJObject { { "data", "data1" } }));
				storage.Batch(accessor => accessor.MapReduce.PutReducedResult(303, "reduceKey1", 1, 2, 3, new RavenJObject { { "data", "data2" } }));
				storage.Batch(accessor => accessor.MapReduce.PutReducedResult(303, "reduceKey2", 2, 3, 4, new RavenJObject { { "data", "data3" } }));
				storage.Batch(accessor => accessor.MapReduce.PutReducedResult(303, "reduceKey2", 2, 4, 5, new RavenJObject { { "data", "data4" } }));
				storage.Batch(accessor => accessor.MapReduce.PutReducedResult(404, "reduceKey1", 1, 2, 3, new RavenJObject { { "data", "data5" } }));

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce
						.GetReducedResultsForDebug(303, "reduceKey1", 1, 0, 10)
						.ToList();

					Assert.Equal(2, results.Count);

					results = accessor.MapReduce
						.GetReducedResultsForDebug(303, "reduceKey2", 2, 0, 10)
						.ToList();

					Assert.Equal(2, results.Count);

					results = accessor.MapReduce
						.GetReducedResultsForDebug(404, "reduceKey1", 1, 0, 10)
						.ToList();

					Assert.Equal(1, results.Count);
				});

				storage.Batch(accessor => accessor.MapReduce.RemoveReduceResults(303, 1, "reduceKey1", 2));

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce
						.GetReducedResultsForDebug(303, "reduceKey1", 1, 0, 10)
						.ToList();

					Assert.Equal(0, results.Count);

					results = accessor.MapReduce
						.GetReducedResultsForDebug(303, "reduceKey2", 2, 0, 10)
						.ToList();

					Assert.Equal(2, results.Count);

					results = accessor.MapReduce
						.GetReducedResultsForDebug(404, "reduceKey1", 1, 0, 10)
						.ToList();

					Assert.Equal(1, results.Count);
				});

				storage.Batch(accessor => accessor.MapReduce.RemoveReduceResults(404, 1, "reduceKey1", 2));

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce
						.GetReducedResultsForDebug(303, "reduceKey1", 1, 0, 10)
						.ToList();

					Assert.Equal(0, results.Count);

					results = accessor.MapReduce
						.GetReducedResultsForDebug(303, "reduceKey2", 2, 0, 10)
						.ToList();

					Assert.Equal(2, results.Count);

					results = accessor.MapReduce
						.GetReducedResultsForDebug(404, "reduceKey1", 1, 0, 10)
						.ToList();

					Assert.Equal(0, results.Count);
				});

				storage.Batch(accessor => accessor.MapReduce.RemoveReduceResults(303, 2, "reduceKey2", 3));

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce
						.GetReducedResultsForDebug(303, "reduceKey1", 1, 0, 10)
						.ToList();

					Assert.Equal(0, results.Count);

					results = accessor.MapReduce
						.GetReducedResultsForDebug(303, "reduceKey2", 2, 0, 10)
						.ToList();

					Assert.Equal(1, results.Count);

					results = accessor.MapReduce
						.GetReducedResultsForDebug(404, "reduceKey1", 1, 0, 10)
						.ToList();

					Assert.Equal(0, results.Count);
				});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void GetReduceTypesPerKeys(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor => Assert.Equal(0, accessor.MapReduce.GetReduceTypesPerKeys(303, 10, 10).Count()));

				storage.Batch(accessor =>
				{
					accessor.MapReduce.ScheduleReductions(303, 0, new ReduceKeyAndBucket(1, "reduceKey1"));
					accessor.MapReduce.ScheduleReductions(303, 0, new ReduceKeyAndBucket(2, "reduceKey1"));
					accessor.MapReduce.ScheduleReductions(303, 0, new ReduceKeyAndBucket(1, "reduceKey2"));
					accessor.MapReduce.ScheduleReductions(404, 0, new ReduceKeyAndBucket(1, "reduceKey1"));
				});

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce
						.GetReduceTypesPerKeys(303, 10, 10)
						.ToList();

					Assert.Equal(2, results.Count);
					Assert.True(results.Any(x => x.ReduceKey == "reduceKey1"));
					Assert.True(results.Any(x => x.ReduceKey == "reduceKey2"));
					Assert.True(results.All(x => x.OperationTypeToPerform == ReduceType.SingleStep));
				});

				storage.Batch(accessor => accessor.MapReduce.IncrementReduceKeyCounter(303, "reduceKey2", 11));

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce
						.GetReduceTypesPerKeys(303, 10, 10)
						.ToList();

					Assert.Equal(2, results.Count);
					Assert.True(results.Any(x => x.ReduceKey == "reduceKey1" && x.OperationTypeToPerform == ReduceType.SingleStep));
					Assert.True(results.Any(x => x.ReduceKey == "reduceKey2" && x.OperationTypeToPerform == ReduceType.MultiStep));

					results = accessor.MapReduce
						.GetReduceTypesPerKeys(303, 10, 15)
						.ToList();

					Assert.Equal(2, results.Count);
					Assert.True(results.Any(x => x.ReduceKey == "reduceKey1" && x.OperationTypeToPerform == ReduceType.SingleStep));
					Assert.True(results.Any(x => x.ReduceKey == "reduceKey2" && x.OperationTypeToPerform == ReduceType.SingleStep));

					results = accessor.MapReduce
						.GetReduceTypesPerKeys(303, 1, 10)
						.ToList();

					Assert.Equal(1, results.Count);

					results = accessor.MapReduce
						.GetReduceTypesPerKeys(303, 0, 10)
						.ToList();

					Assert.Equal(1, results.Count);
				});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void GetLastPerformedReduceType(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor => Assert.Equal(ReduceType.None, accessor.MapReduce.GetLastPerformedReduceType(303, "reduceKey1")));
				storage.Batch(accessor => accessor.MapReduce.UpdatePerformedReduceType(303, "reduceKey1", ReduceType.SingleStep));
				storage.Batch(accessor => Assert.Equal(ReduceType.SingleStep, accessor.MapReduce.GetLastPerformedReduceType(303, "reduceKey1")));
				storage.Batch(accessor => accessor.MapReduce.UpdatePerformedReduceType(303, "reduceKey1", ReduceType.MultiStep));
				storage.Batch(accessor => Assert.Equal(ReduceType.MultiStep, accessor.MapReduce.GetLastPerformedReduceType(303, "reduceKey1")));
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void GetMappedBuckets(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor => Assert.Equal(0, accessor.MapReduce.GetMappedBuckets(303, "reduceKey1").Count()));

				storage.Batch(accessor =>
				{
					accessor.MapReduce.PutMappedResult(303, "doc1", "reduceKey1", new RavenJObject());
					accessor.MapReduce.PutMappedResult(303, "doc2", "reduceKey1", new RavenJObject());
					accessor.MapReduce.PutMappedResult(303, "doc1", "reduceKey2", new RavenJObject());
					accessor.MapReduce.PutMappedResult(404, "doc1", "reduceKey1", new RavenJObject());
				});

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce
						.GetMappedBuckets(303, "reduceKey1")
						.ToList();

					Assert.Equal(2, results.Count);

					results = accessor.MapReduce
						.GetMappedBuckets(303, "reduceKey2")
						.ToList();

					Assert.Equal(1, results.Count);

					results = accessor.MapReduce
						.GetMappedBuckets(404, "reduceKey1")
						.ToList();

					Assert.Equal(1, results.Count);

					results = accessor.MapReduce
						.GetMappedBuckets(404, "reduceKey2")
						.ToList();

					Assert.Equal(0, results.Count);
				});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void GetMappedResults(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor => Assert.Equal(0, accessor.MapReduce.GetMappedResults(303, new List<string> { "reduceKey1" }, true).Count()));
				storage.Batch(accessor =>
				{
					accessor.MapReduce.PutMappedResult(303, "doc1", "reduceKey1", new RavenJObject { { "data", "data1" } });
					accessor.MapReduce.PutMappedResult(303, "doc1", "reduceKey2", new RavenJObject { { "data", "data2" } });
				});

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce
						.GetMappedResults(303, new List<string> { "reduceKey1" }, true)
						.ToList();

					Assert.Equal(1, results.Count);

					var result = results[0];
					Assert.NotEqual(Etag.InvalidEtag, result.Etag);
					Assert.Equal("reduceKey1", result.ReduceKey);
					Assert.True(result.Size > 0);
					Assert.Null(result.Source);
					Assert.True((DateTime.UtcNow - result.Timestamp).TotalMilliseconds < 100);
					Assert.Equal("data1", result.Data["data"]);

					results = accessor.MapReduce
						.GetMappedResults(303, new List<string> { "reduceKey1" }, false)
						.ToList();

					Assert.Equal(1, results.Count);

					result = results[0];
					Assert.Null(result.Data);

					results = accessor.MapReduce
						.GetMappedResults(303, new List<string> { "reduceKey1", "reduceKey2" }, false)
						.ToList();

					Assert.Equal(2, results.Count);
				});
			}
		}
	}
}