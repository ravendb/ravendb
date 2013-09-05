// -----------------------------------------------------------------------
//  <copyright file="MappedResultsStorageActionsTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Tests.Storage.Voron
{
	using System.Linq;

	using Raven.Database.Storage;
	using Raven.Json.Linq;

	using Xunit;
	using Xunit.Extensions;

	public class MappedResultsStorageActionsTests : TransactionalStorageTestBase
	{
		[Theory]
		[PropertyData("Storages")]
		public void IncrementReduceKeyCounterWithValuesEqualOrLessThan0ShouldNotUpdateItemIfItemDoesNotExist(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(
					accessor =>
					{
						var reduceKeysAndTypes = accessor.MapReduce.GetReduceKeysAndTypes("view1", 0, 10).ToList();
						var keyStats = accessor.MapReduce.GetKeysStats("view1", 0, 10).ToList();

						Assert.Equal(0, reduceKeysAndTypes.Count);
						Assert.Equal(0, keyStats.Count);
					});

				storage.Batch(accessor => accessor.MapReduce.IncrementReduceKeyCounter("view1", "reduceKey1", 0));
				storage.Batch(accessor => accessor.MapReduce.IncrementReduceKeyCounter("view1", "reduceKey1", -1));
				storage.Batch(accessor => accessor.MapReduce.IncrementReduceKeyCounter("view1", "reduceKey1", -10));

				storage.Batch(
					accessor =>
					{
						var reduceKeysAndTypes = accessor.MapReduce.GetReduceKeysAndTypes("view1", 0, 10).ToList();
						var keyStats = accessor.MapReduce.GetKeysStats("view1", 0, 10).ToList();

						Assert.Equal(0, reduceKeysAndTypes.Count);
						Assert.Equal(0, keyStats.Count);
					});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void IncrementReduceKeyCounterWithValuesEqualOrLessThan0ShouldUpdateItemIfItemDoesExist(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor => accessor.MapReduce.IncrementReduceKeyCounter("view1", "reduceKey1", 11));

				storage.Batch(
					accessor =>
					{
						var reduceKeysAndTypes = accessor.MapReduce.GetReduceKeysAndTypes("view1", 0, 10).ToList();
						var keyStats = accessor.MapReduce.GetKeysStats("view1", 0, 10).ToList();

						Assert.Equal(0, reduceKeysAndTypes.Count);
						Assert.Equal(1, keyStats.Count);
					});

				storage.Batch(accessor => accessor.MapReduce.IncrementReduceKeyCounter("view1", "reduceKey1", 0));

				storage.Batch(
					accessor =>
					{
						var keyStats = accessor.MapReduce.GetKeysStats("view1", 0, 10).ToList();

						Assert.Equal(1, keyStats.Count);

						var k1 = keyStats[0];
						Assert.Equal(11, k1.Count);
					});

				storage.Batch(accessor => accessor.MapReduce.IncrementReduceKeyCounter("view1", "reduceKey1", -1));

				storage.Batch(
					accessor =>
					{
						var keyStats = accessor.MapReduce.GetKeysStats("view1", 0, 10).ToList();

						Assert.Equal(1, keyStats.Count);

						var k1 = keyStats[0];
						Assert.Equal(10, k1.Count);
					});

				storage.Batch(accessor => accessor.MapReduce.IncrementReduceKeyCounter("view1", "reduceKey1", -10));

				storage.Batch(
					accessor =>
					{
						var keyStats = accessor.MapReduce.GetKeysStats("view1", 0, 10).ToList();

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
				storage.Batch(accessor => accessor.MapReduce.IncrementReduceKeyCounter("view1", "reduceKey1", 7));

				storage.Batch(accessor =>
				{
					var reduceKeysAndTypes = accessor.MapReduce.GetReduceKeysAndTypes("view1", 0, 10).ToList();
					Assert.Equal(0, reduceKeysAndTypes.Count);

					var keyStats = accessor.MapReduce.GetKeysStats("view1", 0, 10).ToList();
					Assert.Equal(1, keyStats.Count);

					var k1 = keyStats[0];
					Assert.Equal("reduceKey1", k1.Key);
					Assert.Equal(7, k1.Count);
				});

				storage.Batch(accessor => accessor.MapReduce.IncrementReduceKeyCounter("view1", "reduceKey1", 3));

				storage.Batch(accessor =>
				{
					var reduceKeysAndTypes = accessor.MapReduce.GetReduceKeysAndTypes("view1", 0, 10).ToList();
					Assert.Equal(0, reduceKeysAndTypes.Count);

					var keyStats = accessor.MapReduce.GetKeysStats("view1", 0, 10).ToList();
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
				storage.Batch(accessor => Assert.Equal(0, accessor.MapReduce.GetReduceKeysAndTypes("view1", 0, 10).Count()));

				storage.Batch(accessor => accessor.MapReduce.UpdatePerformedReduceType("view1", "reduceKey1", ReduceType.SingleStep));
				storage.Batch(accessor => Assert.Equal(1, accessor.MapReduce.GetReduceKeysAndTypes("view1", 0, 10).Count()));

				storage.Batch(accessor => accessor.MapReduce.UpdatePerformedReduceType("view1", "reduceKey1", ReduceType.SingleStep));
				storage.Batch(accessor => Assert.Equal(1, accessor.MapReduce.GetReduceKeysAndTypes("view1", 0, 10).Count()));

				storage.Batch(accessor => accessor.MapReduce.UpdatePerformedReduceType("view1", "reduceKey2", ReduceType.SingleStep));
				storage.Batch(accessor => accessor.MapReduce.UpdatePerformedReduceType("view1", "reduceKey3", ReduceType.SingleStep));
				storage.Batch(accessor => accessor.MapReduce.UpdatePerformedReduceType("view2", "reduceKey4", ReduceType.MultiStep));
				storage.Batch(accessor => Assert.Equal(3, accessor.MapReduce.GetReduceKeysAndTypes("view1", 0, 10).Count()));

				if (requestedStorage == "esent")
				{
					storage.Batch(accessor => accessor.MapReduce.PutMappedResult("view1", "doc1", "reduceKey1", new RavenJObject()));
					storage.Batch(accessor => accessor.MapReduce.PutMappedResult("view1", "doc2", "reduceKey1", new RavenJObject()));
					storage.Batch(accessor => accessor.MapReduce.PutMappedResult("view1", "doc3", "reduceKey1", new RavenJObject()));
					storage.Batch(accessor => accessor.MapReduce.PutMappedResult("view1", "doc4", "reduceKey1", new RavenJObject()));
					storage.Batch(accessor => accessor.MapReduce.PutMappedResult("view1", "doc5", "reduceKey1", new RavenJObject()));
				}

				storage.Batch(accessor =>
				{
					var reduceKeyAndTypes = accessor.MapReduce.GetReduceKeysAndTypes("view1", 0, 1).ToList();
					Assert.Equal(1, reduceKeyAndTypes.Count);
					var k1 = reduceKeyAndTypes[0];

					reduceKeyAndTypes = accessor.MapReduce.GetReduceKeysAndTypes("view1", 1, 1).ToList();
					Assert.Equal(1, reduceKeyAndTypes.Count);
					var k2 = reduceKeyAndTypes[0];

					reduceKeyAndTypes = accessor.MapReduce.GetReduceKeysAndTypes("view1", 2, 1).ToList();
					Assert.Equal(1, reduceKeyAndTypes.Count);
					var k3 = reduceKeyAndTypes[0];

					Assert.NotEqual(k1.ReduceKey, k2.ReduceKey);
					Assert.NotEqual(k1.ReduceKey, k3.ReduceKey);
					Assert.NotEqual(k2.ReduceKey, k3.ReduceKey);

					reduceKeyAndTypes = accessor.MapReduce.GetReduceKeysAndTypes("view1", 1, 2).ToList();
					Assert.Equal(2, reduceKeyAndTypes.Count);
					Assert.Equal(k2.ReduceKey, reduceKeyAndTypes[0].ReduceKey);
					Assert.Equal(k3.ReduceKey, reduceKeyAndTypes[1].ReduceKey);

					reduceKeyAndTypes = accessor.MapReduce.GetReduceKeysAndTypes("view1", 2, 2).ToList();
					Assert.Equal(1, reduceKeyAndTypes.Count);
					Assert.Equal(k3.ReduceKey, reduceKeyAndTypes[0].ReduceKey);

					reduceKeyAndTypes = accessor.MapReduce.GetReduceKeysAndTypes("view1", 0, 5).ToList();
					Assert.Equal(3, reduceKeyAndTypes.Count);
					Assert.Equal(k1.ReduceKey, reduceKeyAndTypes[0].ReduceKey);
					Assert.Equal(k2.ReduceKey, reduceKeyAndTypes[1].ReduceKey);
					Assert.Equal(k3.ReduceKey, reduceKeyAndTypes[2].ReduceKey);

					reduceKeyAndTypes = accessor.MapReduce.GetReduceKeysAndTypes("view1", 5, 55).ToList();
					Assert.Equal(0, reduceKeyAndTypes.Count);

					reduceKeyAndTypes = accessor.MapReduce.GetReduceKeysAndTypes("view2", 0, 10).ToList();
					Assert.Equal(1, reduceKeyAndTypes.Count);
					Assert.Equal("reduceKey4", reduceKeyAndTypes[0].ReduceKey);
				});
			}
		}

		[Fact]
		public void T1()
		{
			using (var storage = NewTransactionalStorage(requestedStorage: "esent"))
			{
				storage.Batch(accessor => accessor.MapReduce.UpdatePerformedReduceType("view1", "reduceKey1", ReduceType.SingleStep));
				storage.Batch(accessor => accessor.MapReduce.UpdatePerformedReduceType("view1", "reduceKey2", ReduceType.SingleStep));
				storage.Batch(accessor => accessor.MapReduce.UpdatePerformedReduceType("view1", "reduceKey3", ReduceType.SingleStep));

				storage.Batch(accessor =>
				{
					var reduceKeyAndTypes = accessor.MapReduce.GetReduceKeysAndTypes("view1", 0, 1).ToList();
					Assert.Equal(1, reduceKeyAndTypes.Count);
					var k1 = reduceKeyAndTypes[0];

					reduceKeyAndTypes = accessor.MapReduce.GetReduceKeysAndTypes("view1", 1, 1).ToList();
					Assert.Equal(1, reduceKeyAndTypes.Count);
					var k2 = reduceKeyAndTypes[0];

					reduceKeyAndTypes = accessor.MapReduce.GetReduceKeysAndTypes("view1", 2, 1).ToList();
					Assert.Equal(1, reduceKeyAndTypes.Count);
					var k3 = reduceKeyAndTypes[0];

					Assert.NotEqual(k1.ReduceKey, k2.ReduceKey);
					Assert.NotEqual(k1.ReduceKey, k3.ReduceKey);
					Assert.NotEqual(k2.ReduceKey, k3.ReduceKey);
				});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void GetKeyStats(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor => Assert.Equal(0, accessor.MapReduce.GetKeysStats("view1", 0, 10).Count()));

				storage.Batch(accessor => accessor.MapReduce.IncrementReduceKeyCounter("view1", "reduceKey1", 7));
				storage.Batch(accessor => Assert.Equal(1, accessor.MapReduce.GetKeysStats("view1", 0, 10).Count()));

				storage.Batch(accessor => accessor.MapReduce.IncrementReduceKeyCounter("view1", "reduceKey1", 7));
				storage.Batch(accessor => Assert.Equal(1, accessor.MapReduce.GetKeysStats("view1", 0, 10).Count()));

				storage.Batch(accessor => accessor.MapReduce.IncrementReduceKeyCounter("view1", "reduceKey2", 7));
				storage.Batch(accessor => accessor.MapReduce.IncrementReduceKeyCounter("view1", "reduceKey3", 7));
				storage.Batch(accessor => accessor.MapReduce.IncrementReduceKeyCounter("view2", "reduceKey1", 7));
				storage.Batch(accessor => Assert.Equal(3, accessor.MapReduce.GetKeysStats("view1", 0, 10).Count()));

				storage.Batch(accessor =>
				{
					var keyStats = accessor.MapReduce.GetKeysStats("view1", 0, 1).ToList();
					Assert.Equal(1, keyStats.Count);
					var k1 = keyStats[0];

					keyStats = accessor.MapReduce.GetKeysStats("view1", 1, 1).ToList();
					Assert.Equal(1, keyStats.Count);
					var k2 = keyStats[0];

					keyStats = accessor.MapReduce.GetKeysStats("view1", 2, 1).ToList();
					Assert.Equal(1, keyStats.Count);
					var k3 = keyStats[0];

					Assert.NotEqual(k1.Key, k2.Key);
					Assert.NotEqual(k1.Key, k3.Key);
					Assert.NotEqual(k2.Key, k3.Key);

					keyStats = accessor.MapReduce.GetKeysStats("view1", 1, 2).ToList();
					Assert.Equal(2, keyStats.Count);
					Assert.Equal(k2.Key, keyStats[0].Key);
					Assert.Equal(k3.Key, keyStats[1].Key);

					keyStats = accessor.MapReduce.GetKeysStats("view1", 2, 2).ToList();
					Assert.Equal(1, keyStats.Count);
					Assert.Equal(k3.Key, keyStats[0].Key);

					keyStats = accessor.MapReduce.GetKeysStats("view1", 0, 5).ToList();
					Assert.Equal(3, keyStats.Count);
					Assert.Equal(k1.Key, keyStats[0].Key);
					Assert.Equal(k2.Key, keyStats[1].Key);
					Assert.Equal(k3.Key, keyStats[2].Key);

					keyStats = accessor.MapReduce.GetKeysStats("view1", 5, 55).ToList();
					Assert.Equal(0, keyStats.Count);

					keyStats = accessor.MapReduce.GetKeysStats("view2", 0, 10).ToList();
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
				storage.Batch(accessor => accessor.MapReduce.UpdatePerformedReduceType("view1", "reduceKey1", ReduceType.None));

				storage.Batch(
					accessor =>
					{
						var keyStats = accessor.MapReduce.GetKeysStats("view1", 0, 10).ToList();
						Assert.Equal(0, keyStats.Count);

						var reduceKeysAndTypes = accessor.MapReduce.GetReduceKeysAndTypes("view1", 0, 10).ToList();
						Assert.Equal(1, reduceKeysAndTypes.Count);
						Assert.Equal("reduceKey1", reduceKeysAndTypes[0].ReduceKey);
						Assert.Equal(ReduceType.None, reduceKeysAndTypes[0].OperationTypeToPerform);
					});

				storage.Batch(accessor => accessor.MapReduce.UpdatePerformedReduceType("view1", "reduceKey1", ReduceType.SingleStep));

				storage.Batch(
					accessor =>
					{
						var keyStats = accessor.MapReduce.GetKeysStats("view1", 0, 10).ToList();
						Assert.Equal(0, keyStats.Count);

						var reduceKeysAndTypes = accessor.MapReduce.GetReduceKeysAndTypes("view1", 0, 10).ToList();
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
				storage.Batch(accessor => accessor.MapReduce.UpdatePerformedReduceType("view1", "reduceKey1", ReduceType.MultiStep));
				storage.Batch(accessor => accessor.MapReduce.IncrementReduceKeyCounter("view1", "reduceKey1", 5));

				storage.Batch(
					accessor =>
					{
						var reduceKeysAndTypes = accessor.MapReduce.GetReduceKeysAndTypes("view1", 0, 10).ToList();
						var keyStats = accessor.MapReduce.GetKeysStats("view1", 0, 10).ToList();

						Assert.Equal(1, reduceKeysAndTypes.Count);
						Assert.Equal("reduceKey1", reduceKeysAndTypes[0].ReduceKey);
						Assert.Equal(ReduceType.MultiStep, reduceKeysAndTypes[0].OperationTypeToPerform);

						Assert.Equal(1, keyStats.Count);
						Assert.Equal("reduceKey1", keyStats[0].Key);
						Assert.Equal(5, keyStats[0].Count);
					});

				storage.Batch(accessor => accessor.MapReduce.IncrementReduceKeyCounter("view2", "reduceKey2", 5));
				storage.Batch(accessor => accessor.MapReduce.UpdatePerformedReduceType("view2", "reduceKey2", ReduceType.MultiStep));

				storage.Batch(
					accessor =>
					{
						var reduceKeysAndTypes = accessor.MapReduce.GetReduceKeysAndTypes("view2", 0, 10).ToList();
						var keyStats = accessor.MapReduce.GetKeysStats("view2", 0, 10).ToList();

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

		}
	}
}