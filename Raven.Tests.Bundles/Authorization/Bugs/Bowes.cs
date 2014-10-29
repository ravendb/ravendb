extern alias client;
using System.Collections.Generic;
using System.Linq;

using Raven.Client;

using Xunit;

namespace Raven.Tests.Bundles.Authorization.Bugs
{
	public class Bowes : AuthorizationTest
	{
		public void SetupData(IDocumentSession session)
		{
			session.Store(
					new client::Raven.Bundles.Authorization.Model.AuthorizationRole
					{
						Id = "Authorization/Roles/Administrator",
						Permissions =
						{
							new client::Raven.Bundles.Authorization.Model.OperationPermission
							{
								Allow=true,
								Operation="OrgDocs/View",
								Tags=new List<string> { "OrgDoc" }
							}
						}
					});

			session.Store(
				new client::Raven.Bundles.Authorization.Model.AuthorizationRole
				{
					Id = "Authorization/Roles/OrganizationUser",
					Permissions =
						{
							new client::Raven.Bundles.Authorization.Model.OperationPermission
							{
								Allow=true,
								Operation="OrgDocs/View",
								Tags=new List<string> { "OrgDoc" }
							}
						}
				});

			session.Store(
				new User
				{
					//A SuperUser which has no Organization
					Id = "Users/1",
					Name = "Admin",
					Roles = { "Authorization/Roles/Administrator" }
				});

			session.Store(
				new User
				{
					Id = "Users/2",
					Name = "Org1User",
					Roles = { "Authorization/Roles/OrganizationUser" },
					OrganizationId = "Organizations/1"
				});

			session.Store(
				new User
				{
					Id = "Users/3",
					Name = "Org2User",
					Roles = { "Authorization/Roles/OrganizationUser" },
					OrganizationId = "Organizations/2"
				});

			session.Store(
				new User
				{
					Id = "Users/4",
					Name = "UnAuthUser"
				});

			session.Store(
				new Organization
				{
					Id = "Organizations/1",
					Name = "TestOrg1"
				});

			session.Store(
				new Organization
				{
					Id = "Organizations/2",
					Name = "TestOrg2"
				});

			session.Store(
				new OrgDoc
				{
					Id = "OrgDocs/1",
					OrganizationId = "Organizations/1",
					Name = "An Org Doc #1 - Org #1"
				});

			session.Store(
				new OrgDoc
				{
					Id = "OrgDocs/2",
					OrganizationId = "Organizations/1",
					Name = "An Org Doc #2 - Org #1"
				});

			session.Store(
				new OrgDoc
				{
					Id = "OrgDocs/3",
					OrganizationId = "Organizations/2",
					Name = "An Org Doc #3 - Org #2"
				});

			session.Store(
				new OrgDoc
				{
					Id = "OrgDocs/4",
					OrganizationId = "Organizations/2",
					Name = "An Org Doc #4 - Org #2"
				});


			client::Raven.Client.Authorization.AuthorizationClientExtensions.SetAuthorizationFor(session, session.Load<OrgDoc>("OrgDocs/1"),
				new client::Raven.Bundles.Authorization.Model.DocumentAuthorization
				{
					Permissions = 
						{
							new client::Raven.Bundles.Authorization.Model.DocumentPermission { Allow=true, Operation="OrgDocs/View",Role="Authorization/Roles/Administrator"}
						},
					Tags = new List<string> { "Organizations/2" }
				});

			client::Raven.Client.Authorization.AuthorizationClientExtensions.SetAuthorizationFor(session, session.Load<OrgDoc>("OrgDocs/2"),
				new client::Raven.Bundles.Authorization.Model.DocumentAuthorization
				{
					Permissions = 
						{
							new client::Raven.Bundles.Authorization.Model.DocumentPermission { Allow=true, Operation="OrgDocs/View",Role="Authorization/Roles/Administrator"}
						},
					Tags = new List<string> { "Organizations/1" }
				});

			client::Raven.Client.Authorization.AuthorizationClientExtensions.SetAuthorizationFor(session, session.Load<OrgDoc>("OrgDocs/3"),
				new client::Raven.Bundles.Authorization.Model.DocumentAuthorization
				{
					Permissions = 
						{
							new client::Raven.Bundles.Authorization.Model.DocumentPermission { Allow=true, Operation="OrgDocs/View",Role="Authorization/Roles/Administrator"}
						},
					Tags = new List<string> { "Organizations/2" }
				});
			client::Raven.Client.Authorization.AuthorizationClientExtensions.SetAuthorizationFor(session,
				session.Load<OrgDoc>("OrgDocs/4"),
				new client::Raven.Bundles.Authorization.Model.DocumentAuthorization
				{
					Permissions = 
						{
							new client::Raven.Bundles.Authorization.Model.DocumentPermission { Allow=true, Operation="OrgDocs/View",Role="Authorization/Roles/Administrator"}
						},
					Tags = new List<string> { "Organizations/2" }
				});

		}

		[Fact]
		public void ShouldWork()
		{
			using (var session = store.OpenSession(DatabaseName))
			{
				SetupData(session);
				session.SaveChanges();
			}

			using (var session = store.OpenSession(DatabaseName))
			{
				client::Raven.Client.Authorization.AuthorizationClientExtensions.SecureFor(session, "Users/4", "OrgDocs/View");

				var vm = session.Query<OrgDoc>().ToList();

				Assert.Empty(vm);
			}
		}
		public class OrgDoc
		{
			public string Id { get; set; }
			public string OrganizationId { get; set; }
			public string Name { get; set; }
		}
		public class Organization
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}
		public class User : client::Raven.Bundles.Authorization.Model.AuthorizationUser
		{
			public string OrganizationId { get; set; }
		}
	}
}