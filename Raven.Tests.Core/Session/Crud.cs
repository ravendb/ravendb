// -----------------------------------------------------------------------
//  <copyright file="CoreTestServer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Dynamic;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace Raven.Tests.Core.Session
{
	public class Crud : RavenCoreTestBase
	{
		[Fact]
		public async Task CanSaveAndLoad()
		{
			using (var store = GetDocumentStore())
			{
				using (var session = store.OpenAsyncSession())
				{
					await session.StoreAsync(new User {Name = "Fitzchak"});
					await session.StoreAsync(new User {Name = "Arek"}, "users/arek");

					await session.SaveChangesAsync();
				}

				using (var session = store.OpenAsyncSession())
				{
					var user = await session.LoadAsync<User>("users/1");
					Assert.NotNull(user);
					Assert.Equal("Fitzchak", user.Name);

					user = await session.LoadAsync<User>("users/arek");
					Assert.NotNull(user);
					Assert.Equal("Arek", user.Name);
				}
			}
		}

		[Fact]
		public async Task CanSaveAndLoadDynamicDocuments()
		{
			using (var store = GetDocumentStore())
			{
				using (var session = store.OpenAsyncSession())
				{
					dynamic user = new ExpandoObject();
					user.Id = "users/1";
					user.Name = "Arek";

					await session.StoreAsync(user);

					await session.SaveChangesAsync();
				}

				using (var session = store.OpenAsyncSession())
				{
					var user = await session.LoadAsync<dynamic>("users/1");

					Assert.NotNull(user);
					Assert.Equal("Arek", user.Name);
				}
			}
		}

		[Fact]
		public async Task CanDelete()
		{
			using (var store = GetDocumentStore())
			{
				using (var session = store.OpenAsyncSession())
				{
					var entity1 = new User {Name = "Andy A"};
					var entity2 = new User {Name = "Andy B"};
					var entity3 = new User {Name = "Andy C"};

					await session.StoreAsync(entity1);
					await session.StoreAsync(entity2);
					await session.StoreAsync(entity3);

					await session.SaveChangesAsync();

					session.Delete(entity1);
					session.Delete("users/2");
					session.Delete<User>(3);

					await session.SaveChangesAsync();

					var users = await session.LoadAsync<User>("users/1", "users/2", "users/3");

					users.ForEach(Assert.Null);
				}
			}
		}

		[Fact]
		public async Task CanLoadWithInclude()
		{
			using (var store = GetDocumentStore())
			{
				using (var session = store.OpenAsyncSession())
				{
					var address = new Address {City = "London", Country = "UK"};
					await session.StoreAsync(address);
					await session.StoreAsync(new User {Name = "Adam", AddressId = session.Advanced.GetDocumentId(address)});

					await session.SaveChangesAsync();
				}

				using (var session = store.OpenAsyncSession())
				{
					var user = await session.Include<User>(x => x.AddressId).LoadAsync<User>("users/1");

					Assert.Equal(1, session.Advanced.NumberOfRequests);

					var address = await session.LoadAsync<Address>(user.AddressId);

					Assert.Equal(1, session.Advanced.NumberOfRequests);
					Assert.NotNull(address);
					Assert.Equal("London", address.City);
				}

				using (var session = store.OpenAsyncSession())
				{
					var user = await session.Include("AddressId").LoadAsync<User>("users/1");

					Assert.Equal(1, session.Advanced.NumberOfRequests);

					var address = await session.LoadAsync<Address>(user.AddressId);

					Assert.Equal(1, session.Advanced.NumberOfRequests);
					Assert.NotNull(address);
					Assert.Equal("London", address.City);
				}
			}
		}
	}
}