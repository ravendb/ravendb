//-----------------------------------------------------------------------
// <copyright file="AuthorizationTest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
extern alias database;

using System;
using System.Collections;
using System.ComponentModel.Composition.Hosting;
using System.Web;
using Raven.Bundles.Authorization;
using Raven.Client.Document;
using Raven.Server;

namespace Raven.Bundles.Tests.Authorization
{
	public abstract class AuthorizationTest : IDisposable
	{
		protected const string UserId = "Authorization/Users/Ayende";
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
			database::Raven.Database.Extensions.IOExtensions.DeleteDirectory("Data");
			database::Raven.Database.Extensions.IOExtensions.DeleteDirectory("Testing");
			database::Raven.Database.Extensions.IOExtensions.DeleteDirectory("Tenants");
			server = new RavenDbServer(new database::Raven.Database.Config.RavenConfiguration
			{
				AnonymousUserAccessMode = database::Raven.Database.Server.AnonymousUserAccessMode.All,
				Catalog = { Catalogs = { new AssemblyCatalog(typeof(AuthorizationDecisions).Assembly) } },
				Port = 8079,
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
