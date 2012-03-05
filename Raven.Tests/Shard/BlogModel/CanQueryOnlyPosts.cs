using System.Linq;
using Raven.Abstractions.Extensions;
using Xunit;

namespace Raven.Tests.Shard.BlogModel
{
	public class CanQueryOnlyPosts : ShardingScenario
	{
		[Fact]
		public void WhenStoringPost()
		{
			using (var session = ShardedDocumentStore.OpenSession())
			{
				session.Store(new Post {Title = "RavenDB is plain awesome"});
				Assert.Equal(2, Servers["Posts01"].Server.NumberOfRequests); // HiLo
				Servers.Where(server => server.Key != "Posts01")
					.ForEach(server => Assert.Equal(0, server.Value.Server.NumberOfRequests));

				session.SaveChanges();
				Assert.Equal(3, Servers["Posts01"].Server.NumberOfRequests);
				Assert.Equal(1, Servers["Posts02"].Server.NumberOfRequests);
				Assert.Equal(1, Servers["Posts03"].Server.NumberOfRequests);
				Servers.Where(server => server.Key.StartsWith("Posts") == false)
					.ForEach(server => Assert.Equal(0, server.Value.Server.NumberOfRequests));
			}
		}
	}
}