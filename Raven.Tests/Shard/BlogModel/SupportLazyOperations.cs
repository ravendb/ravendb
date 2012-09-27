using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Extensions;
using Raven.Client;
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
				         		AssertNumberOfRequests(server.Value, 1);
				         		Assert.Equal(2, server.Value.Database.Statistics.CountOfDocuments);
				         	});
			Servers.Where(server => server.Key != "Users")
				.ForEach(server => AssertNumberOfRequests(server.Value, 0));

			using (var session = ShardedDocumentStore.OpenSession())
			{
				var users = session.Query<User>().Lazily();

				AssertNumberOfRequests(Servers["Users"], 1);
				Servers.Where(server => server.Key != "Users")
					.ForEach(server => AssertNumberOfRequests(server.Value, 0));

				Assert.Equal(2, users.Value.Count());
				AssertNumberOfRequests(Servers["Users"], 2);
				Servers.Where(server => server.Key != "Users")
					.ForEach(server => AssertNumberOfRequests(server.Value, 0));
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
			Servers.ForEach(server => AssertNumberOfRequests(server.Value, 0));
		}

		[Fact]
		public void LazyOperationsAreBatched()
		{
			using (var session = ShardedDocumentStore.OpenSession())
			{
				var result1 = session.Advanced.Lazily.Load<User>("users/1", "users/2");
				var result2 = session.Advanced.Lazily.Load<User>("users/3", "users/4");

				Assert.Equal(new User[2], result2.Value);
				AssertNumberOfRequests(Servers["Users"], 1);
				Servers.Where(server => server.Key != "Users")
					.ForEach(server => AssertNumberOfRequests(server.Value, 0));

				Assert.Equal(new User[2], result1.Value);
				AssertNumberOfRequests(Servers["Users"], 1);
				Servers.Where(server => server.Key != "Users")
					.ForEach(server => AssertNumberOfRequests(server.Value, 0));
			}
		}

		[Fact]
		public void LazyMultiLoadOperationWouldBeInTheSession()
		{
			var ids = new List<string>();
			using (var session = ShardedDocumentStore.OpenSession())
			{
				for (int i = 1; i <= 4; i++)
				{
					var entity = new User {Id = "users/" + i, Name = ids.LastOrDefault()};
					session.Store(entity);
					ids.Add(entity.Id);
				}
				session.SaveChanges();
			}
			AssertNumberOfRequests(Servers["Users"], 1);
			Servers.Where(server => server.Key != "Users")
				.ForEach(server => AssertNumberOfRequests(server.Value, 0));

			using (var session = ShardedDocumentStore.OpenSession())
			{
				var result1 = session.Advanced.Lazily.Load<User>(ids[0], ids[1]);
				var result2 = session.Advanced.Lazily.Load<User>(ids[2], ids[3]);

				var a = result1.Value;
				Assert.Equal(2, a.Length);
				AssertNumberOfRequests(Servers["Users"], 2);
				Servers.Where(server => server.Key != "Users")
					.ForEach(server => AssertNumberOfRequests(server.Value, 0));

				var b = result2.Value;
				Assert.Equal(2, b.Length);
				AssertNumberOfRequests(Servers["Users"], 2);
				Servers.Where(server => server.Key != "Users")
					.ForEach(server => AssertNumberOfRequests(server.Value, 0));

				foreach (var user in b.Concat(a))
				{
					Assert.NotNull(session.Advanced.GetMetadataFor(user));
				}
			}
		}

		[Fact]
		public void LazyLoadOperationWillHandleIncludes()
		{
			var ids = new List<string>();
			using (var session = ShardedDocumentStore.OpenSession())
			{
				for (int i = 1; i <= 4; i++)
				{
					var entity = new User {Name = ids.LastOrDefault()};
					session.Store(entity);
					ids.Add(entity.Id);
				}
				session.SaveChanges();
			}
			using (var session = ShardedDocumentStore.OpenSession())
			{
				var result1 = session.Advanced.Lazily
					.Include("Name")
					.Load<User>(ids[1]);
				var result2 = session.Advanced.Lazily
					.Include("Name")
					.Load<User>(ids[3]);

				Assert.NotNull(result1.Value);
				Assert.NotNull(result2.Value);
				Assert.True(session.Advanced.IsLoaded(result1.Value.Name));
				Assert.True(session.Advanced.IsLoaded(result2.Value.Name));
			}
		}
	}
}