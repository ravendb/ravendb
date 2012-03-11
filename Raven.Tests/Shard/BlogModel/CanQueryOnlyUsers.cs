using System.Linq;
using Raven.Abstractions.Extensions;
using Xunit;

namespace Raven.Tests.Shard.BlogModel
{
	public class CanQueryOnlyUsers : ShardingScenario
	{
		[Fact]
		public void WhenQueryingForUserById()
		{
			using (var session = ShardedDocumentStore.OpenSession())
			{
				var user = session.Load<User>("users/1");
				Assert.Null(user);

				Assert.Equal(1, Servers["Users"].Server.NumberOfRequests);
				Servers.Where(ravenDbServer => ravenDbServer.Key != "Users")
					.ForEach(server => Assert.Equal(0, server.Value.Server.NumberOfRequests));
			}
		}

		[Fact]
		public void WhenQueryingForUsersById()
		{
			using (var session = ShardedDocumentStore.OpenSession())
			{
				var users = session.Load<User>("users/1", "users/2");
				Assert.Equal(2, users.Length);
				Assert.Null(users[0]);
				Assert.Null(users[1]);

				Assert.Equal(1, Servers["Users"].Server.NumberOfRequests);
				Servers.Where(ravenDbServer => ravenDbServer.Key != "Users")
					.ForEach(server => Assert.Equal(0, server.Value.Server.NumberOfRequests));
			}
		}

		[Fact]
		public void WhenStoringUser()
		{
			using (var session = ShardedDocumentStore.OpenSession())
			{
				session.Store(new User { Name = "Fitzchak Yitzchaki" });
				Assert.Equal(2, Servers["Users"].Server.NumberOfRequests); // HiLo
				
				session.SaveChanges();
				Assert.Equal(3, Servers["Users"].Server.NumberOfRequests);
				Servers.Where(ravenDbServer => ravenDbServer.Key != "Users")
					.ForEach(server => Assert.Equal(0, server.Value.Server.NumberOfRequests));
			}

			using (var session = ShardedDocumentStore.OpenSession())
			{
				var user = session.Load<User>("users/1");

				Assert.Equal(4, Servers["Users"].Server.NumberOfRequests);
				Assert.NotNull(user);
				Assert.Equal("Fitzchak Yitzchaki", user.Name);			
				Servers.Where(ravenDbServer => ravenDbServer.Key != "Users")
					.ForEach(server => Assert.Equal(0, server.Value.Server.NumberOfRequests));
			}
		}

		[Fact]
		public void WhenQueryingForUserByName()
		{
			using (var session = ShardedDocumentStore.OpenSession())
			{
				var user = session.Query<User>()
					.FirstOrDefault(x => x.Name == "Fitzchak");
				Assert.Null(user);

				Assert.Equal(1, Servers["Users"].Server.NumberOfRequests);
				Servers.Where(ravenDbServer => ravenDbServer.Key != "Users")
					.ForEach(server => Assert.Equal(0, server.Value.Server.NumberOfRequests));
			}
		}
	}
}