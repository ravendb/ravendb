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
	    private int test = 100;

		[Theory]
		[InlineData("munin")]
		[InlineData("esent")]
		[InlineData("voron")]
		public void NumberOfLoadedItemsToReduceShouldBeLimited(string storageType)
		{
			using (var storage = NewTransactionalStorage(requestedStorage: storageType))
			{
				storage.Batch(accessor =>
				{
					accessor.Indexing.AddIndex(test, true);

					accessor.MapReduce.PutMappedResult(test, "a/1", "a", new RavenJObject() { { "A", "a" } });
					accessor.MapReduce.PutMappedResult(test, "a/1", "a", new RavenJObject() { { "B", "b" } });
					accessor.MapReduce.PutMappedResult(test, "b/1", "b", new RavenJObject() { { "C", "c" } });
					accessor.MapReduce.PutMappedResult(test, "b/1", "b", new RavenJObject() { { "D", "d" } });

					accessor.MapReduce.ScheduleReductions(test, 0,new ReduceKeyAndBucket(IndexingUtil.MapBucket("a/1"), "a"));
					accessor.MapReduce.ScheduleReductions(test, 0, new ReduceKeyAndBucket(IndexingUtil.MapBucket("b/1"), "b"));
				});

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce.GetItemsToReduce(new GetItemsToReduceParams(test, new[] { "a", "b" }, 0, true, new List<object>()){Take = 2}).ToList();
					Assert.Equal(2, results.Count);
					Assert.Equal(results[0].Bucket, results[1].Bucket);
				});
			}
		}

		[Theory]
		[InlineData("munin")]
		[InlineData("esent")]
		[InlineData("voron")]
		public void LimitOfLoadedItemsShouldNotBreakInTheMiddleOfBucket(string storageType)
		{
			using (var storage = NewTransactionalStorage(requestedStorage: storageType))
			{
				storage.Batch(accessor =>
				{
					accessor.Indexing.AddIndex(test, true);

					accessor.MapReduce.PutMappedResult(test, "a/1", "a", new RavenJObject() { { "A", "a" } });
					accessor.MapReduce.PutMappedResult(test, "a/1", "a", new RavenJObject() { { "B", "b" } });
					accessor.MapReduce.PutMappedResult(test, "b/1", "b", new RavenJObject() { { "C", "c" } });
					accessor.MapReduce.PutMappedResult(test, "b/1", "b", new RavenJObject() { { "D", "d" } });

					accessor.MapReduce.ScheduleReductions(test, 0, new ReduceKeyAndBucket(IndexingUtil.MapBucket("a/1"), "a"));
					accessor.MapReduce.ScheduleReductions(test, 0, new ReduceKeyAndBucket(IndexingUtil.MapBucket("b/1"), "b"));
				});

				storage.Batch(accessor =>
				{
					var results = accessor.MapReduce.GetItemsToReduce(new GetItemsToReduceParams(test, new[] { "a", "b" }, 0, true, new List<object>()){Take = 3}).ToList();
					Assert.Equal(4, results.Count);
				});
			}
		}
	}
}