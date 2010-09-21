extern alias database;

using System;
using System.Collections;
using System.ComponentModel.Composition.Hosting;
using System.IO;
using System.Web;
using Raven.Bundles.Authorization;
using Raven.Client.Document;
using Raven.Database;
using Raven.Server;

namespace Raven.Bundles.Tests.Authorization
{
	public abstract class AuthorizationTest : IDisposable
	{
		protected const string UserId = "/Raven/Authorization/Users/Ayende";
		protected DocumentStore store;
		protected RavenDbServer server;

		static AuthorizationTest()
		{
			try
			{
				new Uri("http://fail/first/time?only=%2bplus");
			}
			catch (Exception)
			{
			}
		}

		protected AuthorizationTest()
		{
			if (Directory.Exists("Data"))
				Directory.Delete("Data", true);
			server = new RavenDbServer(new database::Raven.Database.RavenConfiguration
			{
				AnonymousUserAccessMode = database::Raven.Database.AnonymousUserAccessMode.All,
				Catalog = { Catalogs = { new AssemblyCatalog(typeof(AuthorizationDecisions).Assembly) } },
				DataDirectory = "Data",
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
			});
			store = new DocumentStore { Url = server.Database.Configuration.ServerUrl };
			store.Initialize();
			foreach (DictionaryEntry de in HttpRuntime.Cache)
			{
				HttpRuntime.Cache.Remove((string)de.Key);
			}
		}

		public void Dispose()
		{
			store.Dispose();
			server.Dispose();
		}
	}
}