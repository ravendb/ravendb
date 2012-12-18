// -----------------------------------------------------------------------
//  <copyright file="RavenDB_766.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Tests.Issues
{
	using System;
	using System.Collections.Generic;
	using System.Linq;
	using Database.Storage;
	using Raven.Json.Linq;
	using Xunit;
	using Xunit.Extensions;

	public class RavenDB_766 : RavenTest
	{
		[Theory]
		[InlineData("munin")]
		[InlineData("esent")]
		public void ShouldRemoveAllMapResultsAfterDeletingIndex(string storageType)
		{
			Environment.SetEnvironmentVariable("raventest_storage_engine", storageType);

			using (var storage = NewTransactionalStorage())
			{
				storage.Batch(accessor =>
				{
					accessor.Indexing.AddIndex("a", true);
					accessor.Indexing.AddIndex("b", true);

					accessor.MapReduce.PutMappedResult("a", "a/1", "a", new RavenJObject());
					accessor.MapReduce.PutMappedResult("a", "a/2", "a", new RavenJObject());
					accessor.MapReduce.PutMappedResult("b", "a/1", "b", new RavenJObject());
					accessor.MapReduce.PutMappedResult("b", "a/1", "b", new RavenJObject());
				});

				storage.Batch(accessor => accessor.Indexing.DeleteIndex("a"));

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce.GetMappedResultsForDebug("a", "a", 10);
					Assert.Equal(0, results.Count());

					results = accessor.MapReduce.GetMappedResultsForDebug("b", "b", 10);
					Assert.Equal(2, results.Count());
				});
			}
		}

		[Theory]
		[InlineData("munin")]
		[InlineData("esent")]
		public void ShouldRemoveAllReduceResultsAfterDeletingIndex(string storageType)
		{
			Environment.SetEnvironmentVariable("raventest_storage_engine", storageType);

			using (var storage = NewTransactionalStorage())
			{
				storage.Batch(accessor =>
				{
					accessor.Indexing.AddIndex("a", true);
					accessor.Indexing.AddIndex("b", true);

					accessor.MapReduce.PutReducedResult("a", "a", 1, 1, 1, new RavenJObject());
					accessor.MapReduce.PutReducedResult("a", "a", 1, 2, 2, new RavenJObject());
					accessor.MapReduce.PutReducedResult("b", "b", 1, 1, 1, new RavenJObject());
					accessor.MapReduce.PutReducedResult("b", "b", 1, 2, 2, new RavenJObject());
				});

				storage.Batch(accessor => accessor.Indexing.DeleteIndex("a"));

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce.GetReducedResultsForDebug("a", "a", 1, 10);
					Assert.Equal(0, results.Count());

					results = accessor.MapReduce.GetReducedResultsForDebug("b", "b", 1, 10);
					Assert.Equal(2, results.Count());
				});
			}
		}

		[Theory]
		[InlineData("munin")]
		[InlineData("esent")]
		public void ShouldRemoveAllScheduledReductionsAfterDeletingIndex(string storageType)
		{
			Environment.SetEnvironmentVariable("raventest_storage_engine", storageType);

			using (var storage = NewTransactionalStorage())
			{
				storage.Batch(accessor =>
				{
					accessor.Indexing.AddIndex("a", true);
					accessor.Indexing.AddIndex("b", true);

					accessor.MapReduce.ScheduleReductions("a", 1,
														  new List<ReduceKeyAndBucket>()
					                                      {
						                                      new ReduceKeyAndBucket(1, "a"),
						                                      new ReduceKeyAndBucket(2, "a")
					                                      });
					accessor.MapReduce.ScheduleReductions("b", 1, new List<ReduceKeyAndBucket>()
					                                      {
						                                      new ReduceKeyAndBucket(1, "b"),
						                                      new ReduceKeyAndBucket(2, "b")
					                                      });
				});

				storage.Batch(accessor => accessor.Indexing.DeleteIndex("a"));

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce.GetItemsToReduce("a", new[] {"a"},  1, 10, true, new List<object>());
					Assert.Equal(0, results.Count());

					results = accessor.MapReduce.GetItemsToReduce("b", new[] {"b"}, 1, 10, true, new List<object>());
					Assert.Equal(2, results.Count());
				});
			}
		}
	}
}