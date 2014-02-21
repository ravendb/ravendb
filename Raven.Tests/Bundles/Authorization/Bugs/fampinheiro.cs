// //-----------------------------------------------------------------------
// // <copyright file="fampinheiro.cs" company="Hibernating Rhinos LTD">
// //     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// // </copyright>
// //-----------------------------------------------------------------------

extern alias client;
using System.Collections.Generic;

using Raven.Client.Exceptions;

using Xunit;

using System.Linq;

namespace Raven.Tests.Bundles.Authorization.Bugs
{
	public class fampinheiro : AuthorizationTest
	{

		public class Person : client::Raven.Bundles.Authorization.Model.AuthorizationUser
		{
		}

		public class Resource
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		[Fact]
		public void ShouldWork()
		{
			using (var session = store.OpenSession(DatabaseName))
			{
				var person = new Person
				{
					Id = "people/1",
					Name = "Person 1",
					Roles = new List<string>(),
					Permissions = new List<client::Raven.Bundles.Authorization.Model.OperationPermission>()
				};
				session.Store(person);
				person.Permissions.Add(new client::Raven.Bundles.Authorization.Model.OperationPermission()
				{
					Operation = "resources",
					Allow = true,
					Tags = {person.Id}
				});
				person = new Person
				{
					Id = "people/2",
					Name = "Person 2",
					Roles = new List<string>(),
					Permissions = new List<client::Raven.Bundles.Authorization.Model.OperationPermission>()
				};
				session.Store(person);
				person.Permissions.Add(new client::Raven.Bundles.Authorization.Model.OperationPermission()
				{
					Operation = "resources",
					Allow = true,
					Tags = {person.Id}
				});

				var resource = new Resource
				{
					Name = "resources",
					Id = "resources/1"
				};
				session.Store(resource);
				client::Raven.Client.Authorization.AuthorizationClientExtensions.SetAuthorizationFor(session, resource, new client::Raven.Bundles.Authorization.Model.DocumentAuthorization()
				{
					Tags = {"people/1"}
				});
				session.SaveChanges();
			}
			using (var session = store.OpenSession(DatabaseName))
			{
				client::Raven.Client.Authorization.AuthorizationClientExtensions.SecureFor(session, "people/1", "resources/view");
				session.Load<Resource>("resources/1");
				var collection = session.Query<Resource>().ToList();
				Assert.NotEmpty(collection);
			}
			using(var session = store.OpenSession(DatabaseName))
			{
				client::Raven.Client.Authorization.AuthorizationClientExtensions.SecureFor(session, "people/2", "resources/view");
				Assert.Throws<ReadVetoException>(() => session.Load<Resource>("resources/1"));
				var collection = session.Query<Resource>().ToList();
				Assert.Empty(collection);
			}
		}
	}
}