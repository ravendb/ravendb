// -----------------------------------------------------------------------
//  <copyright file="RavenDB_863.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Database.Indexing;
using Raven.Database.Storage;
using Raven.Json.Linq;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
	public class RavenDB_863 : RavenTest
	{
		[Theory]
		[InlineData("munin")]
		[InlineData("esent")]
		public void NumberOfLoadedItemsToReduceShouldBeLimited(string storageType)
		{
			using (var storage = NewTransactionalStorage(requestedStorage: storageType))
			{
				storage.Batch(accessor =>
				{
					accessor.Indexing.AddIndex("test", true);

					accessor.MapReduce.PutMappedResult("test", "a/1", "a", new RavenJObject() { { "A", "a" } });
					accessor.MapReduce.PutMappedResult("test", "a/1", "a", new RavenJObject() { { "B", "b" } });
					accessor.MapReduce.PutMappedResult("test", "b/1", "b", new RavenJObject() { { "C", "c" } });
					accessor.MapReduce.PutMappedResult("test", "b/1", "b", new RavenJObject() { { "D", "d" } });

					accessor.MapReduce.ScheduleReductions("test", 0,
														  new List<ReduceKeyAndBucket>()
														  {
															  new ReduceKeyAndBucket(IndexingUtil.MapBucket("a/1"), "a"),
															  new ReduceKeyAndBucket(IndexingUtil.MapBucket("b/1"), "b")
														  });
				});

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce.GetItemsToReduce("test", new[] { "a", "b" }, 0, true, 2, new List<object>(), new HashSet<Tuple<string, int>>(), new List<string>()).ToList();
					Assert.Equal(2, results.Count);
					Assert.Equal(results[0].Bucket, results[1].Bucket);
				});
			}
		}

		[Theory]
		[InlineData("munin")]
		[InlineData("esent")]
		public void LimitOfLoadedItemsShouldNotBreakInTheMiddleOfBucket(string storageType)
		{
			using (var storage = NewTransactionalStorage(requestedStorage: storageType))
			{
				storage.Batch(accessor =>
				{
					accessor.Indexing.AddIndex("test", true);

					accessor.MapReduce.PutMappedResult("test", "a/1", "a", new RavenJObject() { { "A", "a" } });
					accessor.MapReduce.PutMappedResult("test", "a/1", "a", new RavenJObject() { { "B", "b" } });
					accessor.MapReduce.PutMappedResult("test", "b/1", "b", new RavenJObject() { { "C", "c" } });
					accessor.MapReduce.PutMappedResult("test", "b/1", "b", new RavenJObject() { { "D", "d" } });

					accessor.MapReduce.ScheduleReductions("test", 0,
														  new List<ReduceKeyAndBucket>()
														  {
															  new ReduceKeyAndBucket(IndexingUtil.MapBucket("a/1"), "a"),
															  new ReduceKeyAndBucket(IndexingUtil.MapBucket("b/1"), "b")
														  });
				});

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce.GetItemsToReduce("test", new[] { "a", "b" }, 0, true, 3, new List<object>(), new HashSet<Tuple<string, int>>(), new List<string>()).ToList();
					Assert.Equal(4, results.Count);
				});
			}
		}
	}
}