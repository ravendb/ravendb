//-----------------------------------------------------------------------
// <copyright file="OverwriteIndexRemotely.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Document;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Server;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class OverwriteIndexRemotely : RemoteClientTest, IDisposable
	{
		private readonly RavenDbServer ravenDbServer;
		private readonly IDocumentStore documentStore;

		public OverwriteIndexRemotely()
		{
			const int port = 8079;
			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(8079);

			ravenDbServer = GetNewServer(port);
			documentStore = new DocumentStore {Url = "http://localhost:" + port}.Initialize();
		}

		public override void Dispose()
		{
			documentStore.Dispose();
			ravenDbServer.Dispose();
			base.Dispose();
		}

		[Fact]
		public void CanOverwriteIndex()
		{
			documentStore.DatabaseCommands.PutIndex("test",
			                                        new IndexDefinition
			                                        	{
			                                        		Map = "from doc in docs select new { doc.Name }"
			                                        	}, overwrite: true);


			documentStore.DatabaseCommands.PutIndex("test",
			                                        new IndexDefinition
			                                        	{
			                                        		Map = "from doc in docs select new { doc.Name }"
			                                        	}, overwrite: true);

			documentStore.DatabaseCommands.PutIndex("test",
			                                        new IndexDefinition
			                                        	{
			                                        		Map = "from doc in docs select new { doc.Email }"
			                                        	}, overwrite: true);

			documentStore.DatabaseCommands.PutIndex("test",
			                                        new IndexDefinition
			                                        	{
			                                        		Map = "from doc in docs select new { doc.Email }"
			                                        	}, overwrite: true);
		}
	}
}