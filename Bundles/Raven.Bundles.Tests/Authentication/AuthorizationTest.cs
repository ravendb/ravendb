//-----------------------------------------------------------------------
// <copyright file="AuthorizationTest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
extern alias database;
using System;
using System.Collections;
using System.ComponentModel.Composition.Hosting;
using System.IO;
using System.Reflection;
using System.Web;
using Raven.Bundles.Authentication;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Http;
using Raven.Server;

namespace Raven.Bundles.Tests.Authentication
{
	public abstract class AuthenticationTest : IDisposable
	{
		protected const string UserId = "Raven/Users/Ayende";
		protected DocumentStore store;
		protected EmbeddableDocumentStore embeddedStore;

		static AuthenticationTest()
		{
			try
			{
				new Uri("http://fail/first/time?only=%2bplus");
			}
			catch (Exception)
			{
			}
		}



		protected AuthenticationTest()
		{
			database::Raven.Database.Extensions.IOExtensions.DeleteDirectory("Data");
			embeddedStore = new EmbeddableDocumentStore()
			{
				UseEmbeddedHttpServer = true,
				Configuration =
					{
						AnonymousUserAccessMode = AnonymousUserAccessMode.Get,
						Catalog = {Catalogs = {new AssemblyCatalog(typeof (AuthenticationUser).Assembly)}},
						DataDirectory = "Data",
						RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
						AuthenticationMode = "oauth",
						OAuthTokenCertificate = database::Raven.Database.Config.CertGenerator.GenerateNewCertificate("RavenDB.Test")
					}
			};

			embeddedStore.Initialize();
			store = new DocumentStore { Url = embeddedStore.Configuration.ServerUrl };
			store.Initialize();
			foreach (DictionaryEntry de in HttpRuntime.Cache)
			{
				HttpRuntime.Cache.Remove((string)de.Key);
			}
		}

		public void Dispose()
		{
			store.Dispose();
			embeddedStore.Dispose();
		}
	}
}
