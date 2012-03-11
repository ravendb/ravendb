using Raven.Bundles.Tests.Replication;
using Xunit;

namespace Raven.StressTests.Races
{
	public class BundelsRaceConditions : StressTest
	{
		[Fact]
		public void FailoverBetweenTwoMultiTenantDatabases_CanReplicateBetweenTwoMultiTenantDatabases()
		{
			Run<FailoverBetweenTwoMultiTenantDatabases>(x => x.CanFailoverReplicationBetweenTwoMultiTenantDatabases(), 1000);
		}
		
		[Fact]
		public void FailoverBetweenTwoMultiTenantDatabases_CanFailoverReplicationBetweenTwoMultiTenantDatabases()
		{
			Run<FailoverBetweenTwoMultiTenantDatabases>(x => x.CanFailoverReplicationBetweenTwoMultiTenantDatabases(), 1000);
		}

		[Fact]
		public void FailoverBetweenTwoMultiTenantDatabases_CanFailoverReplicationBetweenTwoMultiTenantDatabases_WithExplicitUrl()
		{
			Run<FailoverBetweenTwoMultiTenantDatabases>(x => x.CanFailoverReplicationBetweenTwoMultiTenantDatabases_WithExplicitUrl(), 1000);
		}
	}
}