using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Extensions;
using Xunit;
using Raven.Client.Linq;

namespace Raven.Tests.Shard.BlogModel
{
	public class SupportLazyOperations : ShardingScenario
	{
		[Fact]
		public void LazyLoadShouldReturnArrayWithNullItems()
		{
			using (var session = ShardedDocumentStore.OpenSession())
			{
				var users = session.Advanced.Lazily.Load<User>("users/1", "users/2");
				Assert.Equal(new User[2], users.Value);
			}
		}

		[Fact]
		public void WithLazyQuery()
		{
			using (var session = ShardedDocumentStore.OpenSession())
			{
				session.Store(new User {Id = "users/1", Name = "Yosef Yitzchak Yitzchaki"});
				session.Store(new User {Id = "users/2", Name = "Fitzchak Yitzchaki"});
				session.SaveChanges();
			}

			Servers.Where(server => server.Key == "Users")
				.ForEach(server =>
				         	{
				         		Assert.Equal(1, server.Value.Server.NumberOfRequests);
				         		Assert.Equal(2, server.Value.Database.Statistics.CountOfDocuments);
				         	});
			Servers.Where(server => server.Key != "Users")
				.ForEach(server => Assert.Equal(0, server.Value.Server.NumberOfRequests));

			Lazy<IEnumerable<User>> users;
			using (var session = ShardedDocumentStore.OpenSession())
			{
				users = session.Query<User>().Lazily();

				Assert.Equal(1, Servers["Users"].Server.NumberOfRequests);
				Servers.Where(server => server.Key != "Users")
					.ForEach(server => Assert.Equal(0, server.Value.Server.NumberOfRequests));

				Assert.Equal(2, users.Value.Count());
				Assert.Equal(2, Servers["Users"].Server.NumberOfRequests);
				Servers.Where(server => server.Key != "Users")
					.ForEach(server => Assert.Equal(0, server.Value.Server.NumberOfRequests));
			}
		}

		[Fact]
		public void UnlessAccessedLazyOpertionsAreNoOp()
		{
			using (var session = ShardedDocumentStore.OpenSession())
			{
				var result1 = session.Advanced.Lazily.Load<User>("users/1", "users/2");
				var result2 = session.Advanced.Lazily.Load<User>("users/3", "users/4");
			}
			Servers.ForEach(server => Assert.Equal(0, server.Value.Server.NumberOfRequests));
		}

		[Fact]
		public void LazyOperationsAreBatched()
		{
			using (var session = ShardedDocumentStore.OpenSession())
			{
				var result1 = session.Advanced.Lazily.Load<User>("users/1", "users/2");
				var result2 = session.Advanced.Lazily.Load<User>("users/3", "users/4");

				Assert.Equal(new User[0], result2.Value);
				Assert.Equal(1, Servers["Users"].Server.NumberOfRequests);
				Servers.Where(server => server.Key != "Users")
					.ForEach(server => Assert.Equal(0, server.Value.Server.NumberOfRequests));

				Assert.Equal(new User[0], result1.Value);
				Assert.Equal(1, Servers["Users"].Server.NumberOfRequests);
				Servers.Where(server => server.Key != "Users")
					.ForEach(server => Assert.Equal(0, server.Value.Server.NumberOfRequests));
			}
		}

		[Fact]
		public void LazyMultiLoadOperationWouldBeInTheSession()
		{
			using (var session = ShardedDocumentStore.OpenSession())
			{
				for (int i = 1; i <= 4; i++)
					session.Store(new User{Id="users/" + i, Name = "User " + i});
				session.SaveChanges();
			}
			Assert.Equal(1, Servers["Users"].Server.NumberOfRequests);
			Servers.Where(server => server.Key != "Users")
				.ForEach(server => Assert.Equal(0, server.Value.Server.NumberOfRequests));

			using (var session = ShardedDocumentStore.OpenSession())
			{
				var result1 = session.Advanced.Lazily.Load<User>("users/1", "users/2");
				var result2 = session.Advanced.Lazily.Load<User>("users/3", "users/4");
				
				var a = result1.Value;
				Assert.Equal(2, a.Length);
				Assert.Equal(2, Servers["Users"].Server.NumberOfRequests);
				Servers.Where(server => server.Key != "Users")
					.ForEach(server => Assert.Equal(0, server.Value.Server.NumberOfRequests));

				var b = result2.Value;
				Assert.Equal(2, b.Length);
				Assert.Equal(2, Servers["Users"].Server.NumberOfRequests);
				Servers.Where(server => server.Key != "Users")
					.ForEach(server => Assert.Equal(0, server.Value.Server.NumberOfRequests));

				foreach (var user in b.Concat(a))
				{
					Assert.NotNull(session.Advanced.GetMetadataFor(user));
				}
			}
		}

		[Fact]
		public void LazyLoadOperationWillHandleIncludes()
		{
			using (var session = ShardedDocumentStore.OpenSession())
			{
				for (int i = 1; i <= 4; i++)
					session.Store(new User { Id = "users/" + i, Name = "users/" + (i + 1) });
				session.SaveChanges();
			}
			using (var session = ShardedDocumentStore.OpenSession())
			{
				var result1 = session.Advanced.Lazily
					.Include("Name")
					.Load<User>("users/1");
				var result2 = session.Advanced.Lazily
					.Include("Name")
					.Load<User>("users/3");

				Assert.NotNull(result1.Value);
				Assert.NotNull(result2.Value);
				Assert.True(session.Advanced.IsLoaded("users/2"));
				Assert.True(session.Advanced.IsLoaded("users/4"));
			}
		}
	}
}