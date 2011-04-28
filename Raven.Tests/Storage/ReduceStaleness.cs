using System;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Indexing;
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

		[Fact]
		public void Esent_when_there_are_multiple_map_results_and_we_ask_for_results()
		{
			using (var transactionalStorage = new EsentTransactionalStorage(new RavenConfiguration
			{
				DataDirectory = "Test"
			}, () => { }))
			{
				when_there_are_multiple_map_results_and_we_ask_for_results(transactionalStorage);
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
				when_there_are_multiple_map_results_and_we_ask_for_results(transactionalStorage);
			}
		}



		private static void when_there_are_multiple_map_results_for_multiple_indexes(ITransactionalStorage transactionalStorage)
		{
			transactionalStorage.Initialize(new DummyUuidGenerator());

			transactionalStorage.Batch(accessor =>
			{
				accessor.Indexing.AddIndex("a",true);
				accessor.Indexing.AddIndex("b", true);
				accessor.Indexing.AddIndex("c", true);

				accessor.MappedResults.PutMappedResult("a", "a/1", "a", new RavenJObject(), MapReduceIndex.ComputeHash("a", "a"));
				accessor.MappedResults.PutMappedResult("a", "a/2", "a", new RavenJObject(), MapReduceIndex.ComputeHash("a", "a"));
				accessor.MappedResults.PutMappedResult("b", "a/1", "a", new RavenJObject(), MapReduceIndex.ComputeHash("b", "a"));
				accessor.MappedResults.PutMappedResult("b", "a/1", "a", new RavenJObject(), MapReduceIndex.ComputeHash("b", "a"));
				accessor.MappedResults.PutMappedResult("c", "a/1", "a", new RavenJObject(), MapReduceIndex.ComputeHash("c", "a"));
				accessor.MappedResults.PutMappedResult("c", "a/1", "a", new RavenJObject(), MapReduceIndex.ComputeHash("c", "a"));
			});

			transactionalStorage.Batch(actionsAccessor =>
			{
				Assert.True(actionsAccessor.Staleness.IsReduceStale("a"));
				Assert.True(actionsAccessor.Staleness.IsReduceStale("b"));
				Assert.True(actionsAccessor.Staleness.IsReduceStale("c"));
			});
		}

		private static void when_there_are_multiple_map_results_and_we_ask_for_results(ITransactionalStorage transactionalStorage)
		{
			transactionalStorage.Initialize(new DummyUuidGenerator());

			transactionalStorage.Batch(accessor =>
			{
				accessor.Indexing.AddIndex("a", true);
				accessor.Indexing.AddIndex("b", true);
				accessor.Indexing.AddIndex("c", true);

				accessor.MappedResults.PutMappedResult("a", "a/1", "a", new RavenJObject(), MapReduceIndex.ComputeHash("a", "a"));
				accessor.MappedResults.PutMappedResult("a", "a/2", "a", new RavenJObject(), MapReduceIndex.ComputeHash("a", "a"));
				accessor.MappedResults.PutMappedResult("b", "a/1", "a", new RavenJObject(), MapReduceIndex.ComputeHash("b", "a"));
				accessor.MappedResults.PutMappedResult("b", "a/1", "a", new RavenJObject(), MapReduceIndex.ComputeHash("b", "a"));
				accessor.MappedResults.PutMappedResult("c", "a/1", "a", new RavenJObject(), MapReduceIndex.ComputeHash("c", "a"));
				accessor.MappedResults.PutMappedResult("c", "a/1", "a", new RavenJObject(), MapReduceIndex.ComputeHash("c", "a"));
			});

			transactionalStorage.Batch(actionsAccessor =>
			{
				Assert.Equal(2, actionsAccessor.MappedResults.GetMappedResultsReduceKeysAfter("a", Guid.Empty).Count());
				Assert.Equal(2, actionsAccessor.MappedResults.GetMappedResultsReduceKeysAfter("b", Guid.Empty).Count());
				Assert.Equal(2, actionsAccessor.MappedResults.GetMappedResultsReduceKeysAfter("c", Guid.Empty).Count());
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