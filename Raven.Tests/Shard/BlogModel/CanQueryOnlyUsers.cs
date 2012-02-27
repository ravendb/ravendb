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

				Assert.Equal(1, servers["Users"].Server.NumberOfRequests);
				foreach (var ravenDbServer in servers)
				{
					if (ravenDbServer.Key == "Users")
						continue;
					Assert.Equal(0, ravenDbServer.Value.Server.NumberOfRequests);
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

			Assert.Equal(1, servers["Users"].Server.NumberOfRequests);
				foreach (var ravenDbServer in servers)
				{
					if (ravenDbServer.Key == "Users")
						continue;
					Assert.Equal(0, ravenDbServer.Value.Server.NumberOfRequests);
				}
			}
		}
	}
}
