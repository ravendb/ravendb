//-----------------------------------------------------------------------
// <copyright file="AuthorizationTest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections;
using System.ComponentModel.Composition.Hosting;
using System.Web;
using Raven.Bundles.Authorization;
using Raven.Client.Document;
using Raven.Database.Extensions;
using Raven.Server;
using Raven.Tests;

namespace Raven.Bundles.Tests.Authorization
{
	public abstract class AuthorizationTest : RavenTest
	{
		protected const string UserId = "Authorization/Users/Ayende";
		protected readonly DocumentStore store;
		protected readonly RavenDbServer server;
		
		protected AuthorizationTest()
		{
			IOExtensions.DeleteDirectory("Data");
			IOExtensions.DeleteDirectory("Testing");
			IOExtensions.DeleteDirectory("Tenants");

			server = GetNewServer(configureServer: configuration => configuration.Catalog.Catalogs.Add(new AssemblyCatalog(typeof (AuthorizationDecisions).Assembly)));
			store = NewRemoteDocumentStore(ravenDbServer: server);
			
			foreach (DictionaryEntry de in HttpRuntime.Cache)
			{
				HttpRuntime.Cache.Remove((string)de.Key);
			}
		}
	}
}