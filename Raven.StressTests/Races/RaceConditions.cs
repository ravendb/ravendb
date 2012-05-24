using Raven.StressTests.Tenants;
using Raven.Tests.Bugs;
using Raven.Tests.Bugs.Caching;
using Raven.Tests.Bugs.Indexing;
using Raven.Tests.MailingList.MapReduceIssue;
using Raven.Tests.ManagedStorage;
using Raven.Tests.MultiGet;
using Raven.Tests.Shard.BlogModel;
using Raven.Tests.Transactions;
using Raven.Tests.Views;
using Xunit;

namespace Raven.StressTests.Races
{
	public class RaceConditions : StressTest
	{
		[Fact]
		public void  SupportLazyOperations_LazyOperationsAreBatched()
		{
			Run<SupportLazyOperations>(x => x.LazyOperationsAreBatched(), 50);
		}

		[Fact]
		public void CanQueryOnlyUsers_WhenQueryingForUserById()
		{
			Run<CanQueryOnlyUsers>(x => x.WhenQueryingForUserById(), 100);
		}

		[Fact]
		public void CanQueryOnlyUsers_WhenStoringUser()
		{
			Run<CanQueryOnlyUsers>(x => x.WhenStoringUser(), 100);
		}

		[Fact]
		public void IndexingEachFieldInEachDocumentSeparetedly()
		{
			Run<IndexingEachFieldInEachDocumentSeparetedly>(x=>x.ForIndexing(), 200);
		}

		[Fact]
		public void ConcurrentlyOpenedTenantsUsingEsent()
		{
			Run<ConcurrentlyOpenedTenantsUsingEsent>(x => x.CanConcurrentlyPutDocsToDifferentTenants());
		}

		[Fact]
		public void CachingOfDocumentInclude()
		{
			Run<CachingOfDocumentInclude>(x => x.New_query_returns_correct_value_when_cache_is_enabled_and_data_changes(), 100);
		}

		[Fact]
		public void CanPageThroughReduceResults()
		{
			Run<CanPageThroughReduceResults>(x => x.Test(), 100);
		}

		[Fact]
		public void MapReduce()
		{
			Run<MapReduce>(x => x.CanUpdateReduceValue_WhenChangingReduceKey(), 100);
		}

		[Fact]
		public void MultiGetNonStaleRequslts()
		{
			Run<MultiGetNonStaleRequslts>(x => x.ShouldBeAbleToGetNonStaleResults());
		}
		
		[Fact]
		public void AfterCommitWillNotRetainSameEtag()
		{
			Run<Etags>(x => x.AfterCommitWillNotRetainSameEtag(), 100);
		}
		
		[Fact]
		public void CanAddAndReadFileAfterReopen()
		{
			Run<Documents>(x => x.CanAddAndReadFileAfterReopen(), 2000);
		}
		
		[Fact]
		public void CanAggressivelyCacheLoads()
		{
			Run<AggressiveCaching>(x => x.CanAggressivelyCacheLoads(), 100);
		}
	}
}