//-----------------------------------------------------------------------
// <copyright file="Mojo2.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
extern alias client;
using System.Collections.Generic;

using Raven.Client;

using Xunit;

namespace Raven.Tests.Bundles.Authorization.Bugs
{
	public class Mojo2_Failed : AuthorizationTest
	{
		private static void SetupRoles(IDocumentSession session)
		{
			session.Store(new client::Raven.Bundles.Authorization.Model.AuthorizationRole
			{
				Id = "Users"
			});

			session.Store(new client::Raven.Bundles.Authorization.Model.AuthorizationRole
			{
				Id = "Administrators",
				Permissions =
					{
						new client::Raven.Bundles.Authorization.Model.OperationPermission
						{
							Allow = true,
							Operation = "Library/Manage"
						}
					}
			});
			session.SaveChanges();
		}

		private static void SetupUsers(IDocumentSession session)
		{
			session.Store(new client::Raven.Bundles.Authorization.Model.AuthorizationUser
			{
				Id = "andrea",
				Roles = { "Users", "Administrators" },
			});

			session.Store(new client::Raven.Bundles.Authorization.Model.AuthorizationUser
			{
				Id = "administrator",
				Roles = { "Users", "Administrators" },
			});

			//Paolo is a Users with permission for Library/Fake
			session.Store(new client::Raven.Bundles.Authorization.Model.AuthorizationUser
			{
				Id = "paolo",
				Roles = { "Users" },
				Permissions =
					new List<client::Raven.Bundles.Authorization.Model.OperationPermission>
					{
						new client::Raven.Bundles.Authorization.Model.OperationPermission
						{Allow = true, Operation = "Library/View"}
					}
			});

			session.SaveChanges();
		}

		[Fact]
		public void Create_Library_And_Set_Permission_For_Roles_Administrators_And_For_User_Andrea()
		{
            using (IDocumentSession session = store.OpenSession(DatabaseName))
			{
				SetupRoles(session);
				SetupUsers(session);
			}
            using (IDocumentSession session = store.OpenSession(DatabaseName))
			{
				var library = new Library { Id = "library/andrea-lib" };
				session.Store(library);
				client::Raven.Client.Authorization.AuthorizationClientExtensions.SetAuthorizationFor(session, library,
																										new client::Raven.Bundles.
																										Authorization.Model.
																										DocumentAuthorization
																										{
																											Permissions =
																											{
																												new client::Raven.Bundles.
																													Authorization.Model.
																													DocumentPermission
																												{
																													Allow = true,
																													Operation = "Library/View",
																													User = "andrea"
																												},
																												new client::Raven.Bundles.
																													Authorization.Model.
																													DocumentPermission
																												{
																													Allow = true,
																													Operation =
																														"Library/Manage",
																													Role = "Administrators"
																												}
																											}
																										});
				;
				session.SaveChanges();
			}

            using (IDocumentSession session = store.OpenSession(DatabaseName))
			{
				var paolo = session.Load<client::Raven.Bundles.Authorization.Model.AuthorizationUser>("paolo");

				//Paolo is a Users
				Assert.True(paolo.Roles.Exists(mc => mc.Equals("Users")));

				//Paolo is not an Administrators
				Assert.True(!paolo.Roles.Exists(mc => mc.Equals("Administrators")));


				client::Raven.Bundles.Authorization.OperationAllowedResult paoloCanView =
					client::Raven.Client.Authorization.AuthorizationClientExtensions.IsOperationAllowedOnDocument(session.Advanced,
																												  "paolo",
																												  "Library/View",
																												  "library/andrea-lib");
				//Paolo can View 
				Assert.True(paoloCanView.IsAllowed);


				client::Raven.Bundles.Authorization.OperationAllowedResult paoloCanMange =
					client::Raven.Client.Authorization.AuthorizationClientExtensions.IsOperationAllowedOnDocument(session.Advanced,
																												  "paolo",
																												  "Library/Manage",
																												  "library/andrea-lib");
				//Paolo cannot Manage
				Assert.True(!paoloCanMange.IsAllowed);
			}
		}

		public class Library
		{
			public string Id { get; set; }
		}
	}
}