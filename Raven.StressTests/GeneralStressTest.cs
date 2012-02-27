using Raven.Tests.Bugs;
using Raven.Tests.Shard.BlogModel;
using Raven.Tests.Views;
using Xunit;

namespace Raven.StressTests
{
	public class GeneralStressTest : StressTest
	{
		[Fact]
		public void MapReduce_CanUpdateReduceValue_WhenChangingReduceKey()
		{
			Run<MapReduce>(storages => storages.CanUpdateReduceValue_WhenChangingReduceKey(), 1000);
		}

		[Fact]
		public void CanQueryOnlyUsers_WhenQueryingForUserById()
		{
			Run<CanQueryOnlyUsers>(storages => storages.WhenQueryingForUserById(), 1000);
		}

		[Fact]
		public void CaseSensitiveDeletes_ShouldWork()
		{
			Run<CaseSensitiveDeletes>(storages => storages.ShouldWork(), 1000);
		}
	}
}