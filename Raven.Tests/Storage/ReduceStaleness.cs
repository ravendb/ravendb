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

namespace Raven.Tests.Storage
{
	public class ReduceStaleness : RavenTest
	{
	    private int a = 100;
	    private int b = 200;
	    private int c = 300;

		[Fact]
		public void when_there_are_multiple_map_results_for_multiple_indexes()
		{
			using(var transactionalStorage = NewTransactionalStorage())
			{
				transactionalStorage.Batch(accessor =>
				{
					accessor.Indexing.AddIndex(a, true);
					accessor.Indexing.AddIndex(b, true);
					accessor.Indexing.AddIndex(c, true);

					accessor.MapReduce.ScheduleReductions(a, 0, new ReduceKeyAndBucket(0, "a"));
					accessor.MapReduce.ScheduleReductions(b, 0, new ReduceKeyAndBucket(0, "a"));
					accessor.MapReduce.ScheduleReductions(c, 0, new ReduceKeyAndBucket(0, "a"));
				});

				transactionalStorage.Batch(actionsAccessor =>
				{
					Assert.True(actionsAccessor.Staleness.IsReduceStale(a));
					Assert.True(actionsAccessor.Staleness.IsReduceStale(b));
					Assert.True(actionsAccessor.Staleness.IsReduceStale(c));
				});
			}
		}
	}
}