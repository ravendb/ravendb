using Raven.Tests.Bugs;
using Raven.Tests.MailingList.MapReduceIssue;
using Raven.Tests.MultiGet;
using Raven.Tests.Views;
using Xunit;

namespace Raven.StressTests.Races
{
	public class RaceConditions : StressTest
	{
		[Fact]
		public void CanPageThroughReduceResults()
		{
			Run<CanPageThroughReduceResults>(x => x.Test());
		}

		[Fact]
		public void MapReduce()
		{
			Run<MapReduce>(x => x.CanUpdateReduceValue_WhenChangingReduceKey());
		}

		[Fact]
		public void MultiGetNonStaleRequslts()
		{
			Run<MultiGetNonStaleRequslts>(x => x.ShouldBeAbleToGetNonStaleResults());
		}

		[Fact]
		public void CaseSensitiveDeletes_ShouldWork()
		{
			Run<CaseSensitiveDeletes>(storages => storages.ShouldWork(), 1000);
		}
	}
}