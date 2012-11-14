using Raven.Tests.Bundles.Replication;
using Raven.Tests.Shard.BlogModel;
using Xunit;

namespace Raven.StressTests.Races
{
	public class BundelsRaceConditions : StressTest
	{
		[Fact]
		public void FailoverBetweenTwoMultiTenantDatabases_CanReplicateBetweenTwoMultiTenantDatabases()
		{
			Run<FailoverBetweenTwoMultiTenantDatabases>(x => x.CanReplicateBetweenTwoMultiTenantDatabases(), 10);
		}
		
		[Fact]
		public void FailoverBetweenTwoMultiTenantDatabases_CanFailoverReplicationBetweenTwoMultiTenantDatabases()
		{
			Run<FailoverBetweenTwoMultiTenantDatabases>(x => x.CanFailoverReplicationBetweenTwoMultiTenantDatabases(), 10);
		}

		[Fact]
		public void FailoverBetweenTwoMultiTenantDatabases_CanFailoverReplicationBetweenTwoMultiTenantDatabases_WithExplicitUrl()
		{
			Run<FailoverBetweenTwoMultiTenantDatabases>(x => x.CanFailoverReplicationBetweenTwoMultiTenantDatabases_WithExplicitUrl(), 10);
		}

		[Fact]
		public void CanMergeResultFromAllPostsShards()
		{
			Run<CanQueryOnlyPosts>(x => x.CanMergeResultFromAllPostsShards(), 10);
		}
		
		[Fact]
		public void SimpleReplication()
		{
			Run<SimpleReplication>(x => x.Can_replicate_between_two_instances(), 10);
		}
	}
}