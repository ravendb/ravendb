using Raven.Tests.Bugs;
using Raven.Tests.Bugs.Caching;
using Raven.Tests.MailingList.MapReduceIssue;
using Raven.Tests.ManagedStorage;
using Raven.Tests.MultiGet;
using Raven.Tests.Transactions;
using Raven.Tests.Views;
using Xunit;

namespace Raven.StressTests.Races
{
	public class RaceConditions : StressTest
	{
		[Fact]
		public void CachingOfDocumentInclude()
		{
			Run<CachingOfDocumentInclude>(x => x.New_query_returns_correct_value_when_cache_is_enabled_and_data_changes());
		}

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
		public void AfterCommitWillNotRetainSameEtag()
		{
			Run<Etags>(x => x.AfterCommitWillNotRetainSameEtag());
		}
		
		[Fact]
		public void CanAddAndReadFileAfterReopen()
		{
			Run<Documents>(x => x.CanAddAndReadFileAfterReopen(), 10000);
		}
		
		[Fact]
		public void CanAggressivelyCacheLoads()
		{
			Run<AggressiveCaching>(x => x.CanAggressivelyCacheLoads(), 10000);
		}
	}
}