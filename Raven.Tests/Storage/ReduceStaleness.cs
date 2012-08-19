using System;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Indexing;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Json.Linq;
using EsentTransactionalStorage = Raven.Storage.Esent.TransactionalStorage;
using MuninTransactionalStorage  = Raven.Storage.Managed.TransactionalStorage;
using Xunit;
using System.Linq;

namespace Raven.Tests.Storage
{
	public class ReduceStaleness : IDisposable
	{
		public ReduceStaleness()
		{
			IOExtensions.DeleteDirectory("Test");
		}

		[Fact]
		public void Esent_when_there_are_multiple_map_results_for_multiple_indexes()
		{
			using(var transactionalStorage = new EsentTransactionalStorage(new RavenConfiguration
			{
				DataDirectory = "Test"
			}, () => { }))
			{
				when_there_are_multiple_map_results_for_multiple_indexes(transactionalStorage);
			}

		}


		[Fact]
		public void Munin_when_there_are_multiple_map_results_for_multiple_indexes()
		{
			using (var transactionalStorage = new MuninTransactionalStorage(new RavenConfiguration
			{
				DataDirectory = "Test"
			}, () => { }))
			{
				when_there_are_multiple_map_results_for_multiple_indexes(transactionalStorage);
			}

		}

		


		private static void when_there_are_multiple_map_results_for_multiple_indexes(ITransactionalStorage transactionalStorage)
		{
			transactionalStorage.Initialize(new DummyUuidGenerator(), new OrderedPartCollection<AbstractDocumentCodec>());

			transactionalStorage.Batch(accessor =>
			{
				accessor.Indexing.AddIndex("a",true);
				accessor.Indexing.AddIndex("b", true);
				accessor.Indexing.AddIndex("c", true);

				accessor.MapReduce.ScheduleReductions("a", 0, new[]{new ReduceKeyAndBucket(0, "a"), });
				accessor.MapReduce.ScheduleReductions("b", 0, new[] { new ReduceKeyAndBucket(0, "a"), });
				accessor.MapReduce.ScheduleReductions("c", 0, new[] { new ReduceKeyAndBucket(0, "a"), });
			});

			transactionalStorage.Batch(actionsAccessor =>
			{
				Assert.True(actionsAccessor.Staleness.IsReduceStale("a"));
				Assert.True(actionsAccessor.Staleness.IsReduceStale("b"));
				Assert.True(actionsAccessor.Staleness.IsReduceStale("c"));
			});
		}

		[Fact]
		public void Esent_when_there_are_multiple_map_results_and_we_ask_for_results()
		{
			using (var transactionalStorage = new EsentTransactionalStorage(new RavenConfiguration
			{
				DataDirectory = "Test"
			}, () => { }))
			{
				when_there_are_multiple_map_results_and_we_ask_for_results_will_get_latest(transactionalStorage);
			}

		}


		[Fact]
		public void Munin_when_there_are_multiple_map_results_and_we_ask_for_results()
		{
			using (var transactionalStorage = new MuninTransactionalStorage(new RavenConfiguration
			{
				DataDirectory = "Test"
			}, () => { }))
			{
				when_there_are_multiple_map_results_and_we_ask_for_results_will_get_latest(transactionalStorage);
			}
		}

		private static void when_there_are_multiple_map_results_and_we_ask_for_results_will_get_latest(ITransactionalStorage transactionalStorage)
		{
			transactionalStorage.Initialize(new DummyUuidGenerator(), new OrderedPartCollection<AbstractDocumentCodec>());

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

		[Fact]
		public void Esent_when_there_are_updates_to_map_reduce_results()
		{
			using (var transactionalStorage = new EsentTransactionalStorage(new RavenConfiguration
			{
				DataDirectory = "Test"
			}, () => { }))
			{
				when_there_are_updates_to_map_reduce_results(transactionalStorage);
			}

		}


		[Fact]
		public void Munin_when_there_are_updates_to_map_reduce_results()
		{
			using (var transactionalStorage = new MuninTransactionalStorage(new RavenConfiguration
			{
				DataDirectory = "Test"
			}, () => { }))
			{
				when_there_are_updates_to_map_reduce_results(transactionalStorage);
			}
		}


		private static void when_there_are_updates_to_map_reduce_results(ITransactionalStorage transactionalStorage)
		{
			var dummyUuidGenerator = new DummyUuidGenerator();
			transactionalStorage.Initialize(dummyUuidGenerator, new OrderedPartCollection<AbstractDocumentCodec>());
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

		/// <summary>
		/// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
		/// </summary>
		/// <filterpriority>2</filterpriority>
		public void Dispose()
		{
			IOExtensions.DeleteDirectory("Test");
		}
	}
}