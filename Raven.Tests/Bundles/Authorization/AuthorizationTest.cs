//-----------------------------------------------------------------------
// <copyright file="AuthorizationTest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.ComponentModel.Composition.Hosting;
using System.Web;
using Raven.Bundles.Authorization;
using Raven.Client.Document;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Server;
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
			IOExtensions.DeleteDirectory("Data");
			IOExtensions.DeleteDirectory("Testing");
			IOExtensions.DeleteDirectory("Tenants");
			server = new RavenDbServer(new RavenConfiguration
			{
				AnonymousUserAccessMode = AnonymousUserAccessMode.Admin,
				Catalog = { Catalogs = { new AssemblyCatalog(typeof(AuthorizationDecisions).Assembly) } },
				Port = 8079,
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
			});
			store = new DocumentStore {Url = server.SystemDatabase.Configuration.ServerUrl };
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
