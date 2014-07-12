// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2172.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;

using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Client.UniqueConstraints;
using Raven.Json.Linq;
using Raven.Tests.Bundles.UniqueConstraints;

using Xunit;

using User = Raven.Tests.Common.Dto.User;

namespace Raven.Tests.Issues
{
	public class RavenDB_2172 : UniqueConstraintsTest
	{
		private class UserIndex : AbstractIndexCreationTask<User>
		{
			public UserIndex()
			{
				Map = users => from user in users
							   select new
									  {
										  user.Active,
										  user.Age,
										  user.Name
									  };

				StoreAllFields(FieldStorage.Yes);
			}
		}

		private class UserProjection
		{
#pragma warning disable 649
			public bool Active;

			public int Age;

			public string Name;
#pragma warning restore 649
		}

		private class UserWithIdAsField
		{
#pragma warning disable 649
			public string Id;
#pragma warning restore 649

			public bool Active;

			[UniqueConstraint]
			public int Age;

			public string Name;
		}

		[Fact]
		public void AsProjectionShouldWorkOnFields()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User { Active = true, Age = 10, Name = "Name1" });
					session.Store(new User { Active = false, Age = 20, Name = "Name2" });

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var projection = session.Query<User>()
							.Customize(x => x.WaitForNonStaleResults())
							.AsProjection<UserProjection>()
							.ToList();

					Assert.Equal(2, projection.Count);
					Assert.True(projection.Any(x => x.Age == 10 && x.Name == "Name1" && x.Active));
					Assert.True(projection.Any(x => x.Age == 20 && x.Name == "Name2" && x.Active == false));
				}
			}
		}

		[Fact]
		public void IdentityCanBeAField()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					var user1 = new UserWithIdAsField { Active = true, Age = 10, Name = "Name1" };
					var user2 = new UserWithIdAsField { Active = false, Age = 20, Name = "Name2" };

					session.Store(user1);
					session.Store(user2);

					Assert.Equal("UserWithIdAsFields/1", user1.Id);
					Assert.Equal("UserWithIdAsFields/2", user2.Id);
				}
			}
		}

		[Fact]
		public void SelectFieldsShouldWorkOnFields()
		{
			using (var store = NewDocumentStore())
			{
				new UserIndex().Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new User { Active = true, Age = 10, Name = "Name1" });
					session.Store(new User { Active = false, Age = 20, Name = "Name2" });

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var projection = session
						.Advanced
						.DocumentQuery<User>(new UserIndex().IndexName)
						.SelectFields<UserProjection>()
						.WaitForNonStaleResults()
						.ToList();

					Assert.Equal(2, projection.Count);
					Assert.True(projection.Any(x => x.Age == 10 && x.Name == "Name1" && x.Active));
					Assert.True(projection.Any(x => x.Age == 20 && x.Name == "Name2" && x.Active == false));
				}
			}
		}

		[Fact]
		public void RefreshShouldWorkOnFields()
		{
			using (var store = NewDocumentStore())
			{
				new UserIndex().Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new UserWithIdAsField { Active = true, Age = 10, Name = "Name1" });

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var user = session.Load<UserWithIdAsField>(1);
					var userId = user.Id;
					var metadata = (RavenJObject)session.Advanced.GetMetadataFor(user).CloneToken();
					metadata.Remove("@etag");

					store.DatabaseCommands.Put(userId, null, RavenJObject.FromObject(new UserWithIdAsField { Active = false, Age = 20, Name = "Name2" }), metadata);

					session.Advanced.Refresh(user);

					Assert.Equal(userId, user.Id);
					Assert.Equal(false, user.Active);
					Assert.Equal(20, user.Age);
					Assert.Equal("Name2", user.Name);
				}
			}
		}

		[Fact]
		public void UniqueConstraintShouldWorkOnFields()
		{
			using (var session = DocumentStore.OpenSession())
			{
				session.Store(new UserWithIdAsField { Active = true, Age = 10, Name = "Name1" });
				session.Store(new UserWithIdAsField { Active = false, Age = 20, Name = "Name2" });

				session.SaveChanges();
			}

			using (var session = DocumentStore.OpenSession())
			{
				var user = session.LoadByUniqueConstraint<UserWithIdAsField>(x => x.Age, 10);

				Assert.Equal("UserWithIdAsFields/1", user.Id);
				Assert.Equal(true, user.Active);
				Assert.Equal(10, user.Age);
				Assert.Equal("Name1", user.Name);
			}
		}
	}
}