// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1217.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_1217 : RavenTest
	{
		[Fact]
		public void CanReadFailoverServersFromConnectionString()
		{
			using (var documentStore = new DocumentStore { ConnectionStringName = "FailoverServers" })
			{
				Assert.NotNull(documentStore.FailoverServers);
				Assert.Equal("http://localhost:8078", documentStore.FailoverServers.ForDefaultDatabase[0]);
				Assert.Equal("http://localhost:8077/databases/test", documentStore.FailoverServers.ForDefaultDatabase[1]);
				Assert.Equal("http://localhost:8076", documentStore.FailoverServers.GetForDatabase("Northwind")[0]);
			}
		}

		[Fact]
		public void ReplicationInformerCanUseFailoverServersConfiguredInConnectionString()
		{
			using (var store = new DocumentStore() { ConnectionStringName = "FailoverServers" }.Initialize())
			{
				// for default database
				var serverClient = (ServerClient) store.DatabaseCommands;
				
				serverClient.ReplicationInformer.RefreshReplicationInformation(serverClient);
				var servers = serverClient.ReplicationInformer.ReplicationDestinations; 

				Assert.Equal(2, servers.Count);
				Assert.Equal("http://localhost:8078", servers[0].Url);
				Assert.Equal("http://localhost:8077/databases/test", servers[1].Url);

				// for Northwind database configured in App.config
				serverClient = (ServerClient) store.DatabaseCommands.ForDatabase("Northwind");
				serverClient.ReplicationInformer.RefreshReplicationInformation(serverClient);

				servers = serverClient.ReplicationInformer.ReplicationDestinations;

				Assert.Equal(1, servers.Count);
				Assert.Equal("http://localhost:8076", servers[0].Url);
			}
		}

		[Fact]
		public void ReplicationInformerCanUseFailoverServersConfiguredInCodeWhenStoreInitialized()
		{
			using (var store = new DocumentStore()
			{
				Url = "http://localhost:1234",
				FailoverServers = new FailoverServers
				{
					ForDefaultDatabase = new[]
					{
						"http://localhost:8078",
						"http://localhost:8077/databases/test"
					},
					ForDatabases = new Dictionary<string, string[]>
					{
						{"Northwind", new [] {"http://localhost:8076"}}
					}
				}
			}.Initialize())
			{
				// for default database
				var serverClient = (ServerClient)store.DatabaseCommands;

				serverClient.ReplicationInformer.RefreshReplicationInformation(serverClient);
				var servers = serverClient.ReplicationInformer.ReplicationDestinations;

				Assert.Equal(2, servers.Count);
				Assert.Equal("http://localhost:8078", servers[0].Url);
				Assert.Equal("http://localhost:8077/databases/test", servers[1].Url);

				// for Northwind database
				serverClient = (ServerClient)store.DatabaseCommands.ForDatabase("Northwind");
				serverClient.ReplicationInformer.RefreshReplicationInformation(serverClient);

				servers = serverClient.ReplicationInformer.ReplicationDestinations;

				Assert.Equal(1, servers.Count);
				Assert.Equal("http://localhost:8076", servers[0].Url);
			}
		}
	}
}