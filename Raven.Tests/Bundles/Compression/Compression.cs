//-----------------------------------------------------------------------
// <copyright file="Expiration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using System.Reflection;
using Raven.Client.Document;
using Raven.Server;

namespace Raven.Tests.Bundles.Compression
{
	public abstract class Compression : IDisposable
	{
		protected readonly string path;
		protected readonly DocumentStore documentStore;
		private readonly RavenDbServer ravenDbServer;
		private bool closed = false;

		public Compression()
		{
			path = Path.GetDirectoryName(Assembly.GetAssembly(typeof(Versioning.Versioning)).CodeBase);
			path = Path.Combine(path, "TestDb").Substring(6);
			Raven.Database.Extensions.IOExtensions.DeleteDirectory(path);
			var config = new Raven.Database.Config.RavenConfiguration
			             	{
			             		Port = 8079,
			             		RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
			             		DataDirectory = path,
								Settings = {{"Raven/ActiveBundles", "Compression"}}
			             	};
			config.PostInit();
			ravenDbServer = new RavenDbServer(config);
			documentStore = new DocumentStore
			{
				Url = "http://localhost:8079"
			};
			documentStore.Initialize();
		}

		protected void AssertPlainTextIsNotSavedInDatabase_ExceptIndexes(params string[] plaintext)
		{
			Close();
			TestUtil.AssertPlainTextIsNotSavedInAnyFileInPath(plaintext, path, 
				file => Path.GetExtension(file) != ".cfs");
		}

		protected void Close()
		{
			if (closed)
				return;

			documentStore.Dispose();
			ravenDbServer.Dispose();
			closed = true;
		}

		public void Dispose()
		{
			Close();
			Raven.Database.Extensions.IOExtensions.DeleteDirectory(path);
		}
	}
}