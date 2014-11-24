// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1217.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Net;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Tests.Common;

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
				Assert.Equal("http://localhost:8078", documentStore.FailoverServers.ForDefaultDatabase[0].Url);
				Assert.Equal("http://localhost:8077", documentStore.FailoverServers.ForDefaultDatabase[1].Url);
				Assert.Equal("test", documentStore.FailoverServers.ForDefaultDatabase[1].Database);
				Assert.Equal("http://localhost:8076", documentStore.FailoverServers.GetForDatabase("Northwind")[0].Url);
				Assert.Equal("http://localhost:8075", documentStore.FailoverServers.ForDefaultDatabase[2].Url);
				Assert.Equal("user", documentStore.FailoverServers.ForDefaultDatabase[2].Username);
				Assert.Equal("secret", documentStore.FailoverServers.ForDefaultDatabase[2].Password);
				Assert.Equal("http://localhost:8074", documentStore.FailoverServers.ForDefaultDatabase[3].Url);
				Assert.Equal("d5723e19-92ad-4531-adad-8611e6e05c8a", documentStore.FailoverServers.ForDefaultDatabase[3].ApiKey);
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

				Assert.Equal(4, servers.Count);
				Assert.Equal("http://localhost:8078", servers[0].Url);
				Assert.Equal("http://localhost:8077/databases/test/", servers[1].Url);
				Assert.Equal("http://localhost:8075", servers[2].Url);
				Assert.Equal("user", (servers[2].Credentials.Credentials as NetworkCredential).UserName);
				Assert.Equal("secret", (servers[2].Credentials.Credentials as NetworkCredential).Password);
				Assert.Equal("http://localhost:8074", servers[3].Url);
				Assert.Equal("d5723e19-92ad-4531-adad-8611e6e05c8a", servers[3].Credentials.ApiKey);

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
                Url = "http://localhost:59234", // do not change this
				FailoverServers = new FailoverServers
				{
					ForDefaultDatabase = new[]
					{
						new ReplicationDestination { Url = "http://localhost:8078", ApiKey = "apikey"},
						new ReplicationDestination { Url = "http://localhost:8077/", Database = "test", Username = "user", Password = "secret"}
					},
					ForDatabases = new Dictionary<string, ReplicationDestination[]>
					{
						{"Northwind", new []
										{
											new ReplicationDestination { Url = "http://localhost:8076"}
										}
						}
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
				Assert.Equal("apikey", servers[0].Credentials.ApiKey);
				Assert.Equal("http://localhost:8077/databases/test/", servers[1].Url);
				Assert.Equal("user", (servers[1].Credentials.Credentials as NetworkCredential).UserName);
				Assert.Equal("secret", (servers[1].Credentials.Credentials as NetworkCredential).Password);

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