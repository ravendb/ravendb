using System.Linq;
using Xunit;

namespace Raven.Tests.Shard.BlogModel
{
	public class CanQueryOnlyUsers : ShardingScenario
	{
		[Fact]
		public void WhenQueryingForUserById()
		{
			using (var session = shardedDocumentStore.OpenSession())
			{
				var user = session.Load<User>("users/1");

				// one request for the replication destination support
				Assert.Equal(2, servers["Users"].Server.NumberOfRequests);
				foreach (var ravenDbServer in servers)
				{
					if (ravenDbServer.Key == "Users")
						continue;
					Assert.Equal(1, ravenDbServer.Value.Server.NumberOfRequests);
				}
			}
		}

		[Fact]
		public void WhenQueryingForUserByName()
		{
			using (var session = shardedDocumentStore.OpenSession())
			{
				var user = session.Query<User>()
					.FirstOrDefault(x => x.Name == "Fitzchak");

				// one request for the replication destination support
				Assert.Equal(2, servers["Users"].Server.NumberOfRequests);
				foreach (var ravenDbServer in servers)
				{
					if (ravenDbServer.Key == "Users")
						continue;
					Assert.Equal(1, ravenDbServer.Value.Server.NumberOfRequests);
				}
			}
		}
	}
}