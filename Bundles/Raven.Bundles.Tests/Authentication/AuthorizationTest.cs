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
using Raven.Http;
using Raven.Server;

namespace Raven.Bundles.Tests.Authentication
{
	public abstract class AuthenticationTest : IDisposable
	{
		protected const string UserId = "Raven/Users/Ayende";
		protected DocumentStore store;
		protected RavenDbServer server;

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


		protected string GetPath(string subFolderName)
		{
			string retPath = Path.GetDirectoryName(Assembly.GetAssembly(typeof(AuthenticationTest)).CodeBase);
			return Path.Combine(retPath, subFolderName).Substring(6); //remove leading file://
		}

		protected AuthenticationTest()
		{
			database::Raven.Database.Extensions.IOExtensions.DeleteDirectory("Data");
            server = new RavenDbServer(new database::Raven.Database.Config.RavenConfiguration
			{
				AnonymousUserAccessMode = AnonymousUserAccessMode.All,
				Catalog = { Catalogs = { new AssemblyCatalog(typeof(AuthenticationUser).Assembly) } },
				DataDirectory = "Data",
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
				OAuthTokenCertificatePath = GetPath(@"Authentication\Private.pfx"),
				OAuthTokenCertificatePassword = "Password123"
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
