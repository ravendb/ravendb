//-----------------------------------------------------------------------
// <copyright file="Expiration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Client.Document;
using Raven.Server;
using Raven.Tests.Common;

namespace Raven.Tests.Bundles.Encryption
{
	public abstract class Encryption : RavenTest
	{
		private readonly string path;
		protected readonly DocumentStore documentStore;
		private RavenDbServer ravenDbServer;
		private bool closed = false;

		public Encryption()
		{
			path = NewDataPath();
			createServer();
			documentStore = NewRemoteDocumentStore(ravenDbServer: ravenDbServer);
		}

		private void createServer()
		{
            ravenDbServer = GetNewServer(runInMemory: false, dataDirectory: path, activeBundles: "Encryption", configureConfig: configuration =>
			{
				configuration.Settings["Raven/Encryption/Key"] = "3w17MIVIBLSWZpzH0YarqRlR2+yHiv1Zq3TCWXLEMI8=";
			});
		}


		protected void AssertPlainTextIsNotSavedInDatabase(params string[] plaintext)
		{
			Close();
			TestUtil.AssertPlainTextIsNotSavedInAnyFileInPath(plaintext, path, s => true);
		}

		protected void RecycleServer()
		{
			ravenDbServer.Dispose();
			createServer();
		}

		protected void Close()
		{
			if (closed)
				return;

			documentStore.Dispose();
			ravenDbServer.Dispose();
			closed = true;
		}

		public override void Dispose()
		{
			Close();
			base.Dispose();
		}
	}
}