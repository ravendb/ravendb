using System;
using System.ComponentModel.Composition.Hosting;
using Newtonsoft.Json.Linq;
using Raven.Bundles.Authorization;
using Raven.Bundles.Authorization.Model;
using Raven.Client.Document;
using Raven.Database;
using Raven.Server;
using Xunit;
using Raven.Client.Authorization;

namespace Raven.Bundles.Tests.Authorization
{
	public class CanHandleAuthQuestions : IDisposable
	{
		private readonly DocumentStore store;
		private readonly RavenDbServer server;
		private readonly AuthorizationDecisions authorizationDecisions;
		const string userId = "/Raven/Authorization/Users/Ayende";
		private const string operation = "/Company/Solicit";

		public CanHandleAuthQuestions()
		{
			server = new RavenDbServer(new RavenConfiguration
			{
				AnonymousUserAccessMode = AnonymousUserAccessMode.All,
				Catalog = { Catalogs = { new AssemblyCatalog(typeof(AuthorizationDecisions).Assembly) } },
				DataDirectory = "Data",
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
			});
			store = new DocumentStore { Url = server.Database.Configuration.ServerUrl };
			store.Initialize();
			authorizationDecisions = new AuthorizationDecisions(server.Database);
		}

		[Fact]
		public void CanGiveUserExplicitPermissionOnDoc()
		{
			var company = new Company
			{
				Name = "Hibernating Rhinos"
			};
			using (var s = store.OpenSession())
			{
				s.Store(new AuthorizationUser
				{
					Id = userId,
					Name = "Ayende Rahien",
				});
				
				s.Store(company);

				s.SetAuthorizationFor(company, new DocumentAuthorization
				{
					Permissions =
						{
							new DocumentPermission
							{
								Allow = true,
								Operation = operation,
								User = userId
							}
						}
				});

				s.SaveChanges();
			}

			var jsonDocument = server.Database.Get(company.Id, null);
			var isAllowed = authorizationDecisions.IsAllowed(userId, operation, company.Id, jsonDocument.Metadata, null);
			Assert.True(isAllowed);
		}

		public void Dispose()
		{
			store.Dispose();
			server.Dispose();
		}
	}
}