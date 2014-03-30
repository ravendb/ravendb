// -----------------------------------------------------------------------
//  <copyright file="RavenDB_381.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using Raven.Abstractions.Util;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_381 : RavenTest
	{
		[Fact]
		public void CanChangeConventionJustForOneType()
		{
			using(var store = NewDocumentStore())
			{
				store.Conventions.RegisterIdConvention<User>((dbName, cmds, user) => "users/" + user.Name);

				using(var session = store.OpenSession())
				{
					var entity = new User {Name = "Ayende"};
					session.Store(entity);

					Assert.Equal("users/Ayende", session.Advanced.GetDocumentId(entity));
				}
			}
		}

		[Fact]
		public async Task CanChangeConventionJustForOneType_Async()
		{
			using(GetNewServer())
			using (var store = new DocumentStore{Url = "http://localhost:8079"}.Initialize())
			{
				store.Conventions.RegisterAsyncIdConvention<User>((dbName, cmds, user) => new CompletedTask<string>("users/" + user.Name));

				using (var session = store.OpenAsyncSession())
				{
					var entity = new User { Name = "Ayende" };
					await session.StoreAsync(entity);
					await session.SaveChangesAsync();
					Assert.Equal("users/Ayende", session.Advanced.GetDocumentId(entity));
				}
			}
		}

		public class User
		{
			public string Name { get; set; }
		}
	}
}