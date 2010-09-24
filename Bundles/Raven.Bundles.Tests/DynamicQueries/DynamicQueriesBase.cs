extern alias database;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Document;
using Raven.Server;
using System.IO;
using System.ComponentModel.Composition.Hosting;
using Raven.Bundles.Authorization;
using System.Web;
using System.Collections;
using Raven.Bundles.DynamicQueries.Responders;

namespace Raven.Bundles.Tests.DynamicQueries
{
    public class DynamicQueriesBase
    {
		protected DocumentStore store;
		protected RavenDbServer server;

		static DynamicQueriesBase()
		{
			try
			{
				new Uri("http://fail/first/time?only=%2bplus");
			}
			catch (Exception)
			{
			}
		}

        protected DynamicQueriesBase()
		{
			if (Directory.Exists("Data"))
				Directory.Delete("Data", true);
			server = new RavenDbServer(new database::Raven.Database.RavenConfiguration
			{
				AnonymousUserAccessMode = database::Raven.Database.AnonymousUserAccessMode.All,
				Catalog = { Catalogs = { new AssemblyCatalog(typeof(DynamicResponder).Assembly) } },
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
