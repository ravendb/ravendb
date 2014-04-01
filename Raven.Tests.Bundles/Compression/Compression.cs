//-----------------------------------------------------------------------
// <copyright file="Compression.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.IO;
using Raven.Client.Document;
using Raven.Server;
using Raven.Tests.Common;

namespace Raven.Tests.Bundles.Compression
{
	public abstract class Compression : RavenTest
	{
		private readonly string path;
		private readonly RavenDbServer ravenDbServer;
		protected readonly DocumentStore documentStore;

		public Compression()
		{
			// This will be disposed by the RavenTestBase.Dispose method
			path = NewDataPath("Compression");
			ravenDbServer = GetNewServer(activeBundles: "Compression", dataDirectory: path, runInMemory:false);
			documentStore = NewRemoteDocumentStore(ravenDbServer: ravenDbServer);
		}

		protected void AssertPlainTextIsNotSavedInDatabase_ExceptIndexes(params string[] plaintext)
		{
			documentStore.Dispose();
			ravenDbServer.Dispose();
			TestUtil.AssertPlainTextIsNotSavedInAnyFileInPath(plaintext, path, file => Path.GetExtension(file) != ".cfs");
		}
	}
}