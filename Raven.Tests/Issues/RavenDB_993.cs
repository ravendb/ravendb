// -----------------------------------------------------------------------
//  <copyright file="RavenDB_993.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Net;
using Lucene.Net.Support;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Database.Extensions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Server;
using Raven.Tests.Bundles.Replication;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_993 : ReplicationBase
	{
		private RavenDbServer server1;
		private RavenDbServer server2;
		private DocumentStore store1;
		private DocumentStore store2;

		[Fact]
		public void ValidReplicationSetupTest()
		{
			server1 = CreateServer(8111, "D1");
			server2 = CreateServer(8112, "D2");

			store1 = new DocumentStore
			{
				DefaultDatabase = "Northwind",
				Url = "http://localhost:8111"
			};

			store1.Initialize();

			store2 = new DocumentStore
			{
				DefaultDatabase = "Northwind",
				Url = "http://localhost:8112"
			};

			store2.Initialize();

			store1.DatabaseCommands.GlobalAdmin.CreateDatabase(
				new DatabaseDocument
				{
					Id = "Northwind",
					Settings = { { "Raven/ActiveBundles", "replication" }, { "Raven/DataDir", @"~\D1\N" } }
				});

			store2.DatabaseCommands.GlobalAdmin.CreateDatabase(
				new DatabaseDocument
				{
					Id = "Northwind",
					Settings = { { "Raven/ActiveBundles", "replication" }, { "Raven/DataDir", @"~\D2\N" } }
				});

			var db1Url = store1.Url + "/databases/Northwind";
			var db2Url = store2.Url + "/databases/Northwind";

			SetupReplication(store1.DatabaseCommands, db2Url);

			var replicationDocument = store1.DatabaseCommands.Get("Raven/Replication/Destinations");

			var request = store1.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null,
			                                                                                db1Url + "/admin/replicationInfo",
																							"POST", CredentialCache.DefaultCredentials, store1.Conventions));

			request.Write(replicationDocument.DataAsJson.ToString(Formatting.None));
			var result = request.ReadResponseJson() as RavenJArray;

			Assert.NotNull(result);
			Assert.Equal(1, result.Length);
			Assert.Equal("Valid", result[0].Value<string>("Status"));
			Assert.Equal(200, result[0].Value<int>("Code"));
		}

		[Fact]
		public void DestinationServerWithReplicationDisabledTest()
		{
			server1 = CreateServer(8111, "D1");
			server2 = CreateServer(8112, "D2");

			store1 = new DocumentStore
			{
				DefaultDatabase = "Northwind",
				Url = "http://localhost:8111"
			};

			store1.Initialize();

			store2 = new DocumentStore
			{
				DefaultDatabase = "Northwind",
				Url = "http://localhost:8112"
			};

			store2.Initialize();

			store1.DatabaseCommands.GlobalAdmin.CreateDatabase(
				new DatabaseDocument
				{
					Id = "Northwind",
					Settings = { { "Raven/ActiveBundles", "replication" }, { "Raven/DataDir", @"~\D1\N" } }
				});

			store2.DatabaseCommands.GlobalAdmin.CreateDatabase(
				new DatabaseDocument
				{
					Id = "Northwind",
					Settings = { { "Raven/ActiveBundles", "" }, { "Raven/DataDir", @"~\D2\N" } }
				});

			var db1Url = store1.Url + "/databases/Northwind";
			var db2Url = store2.Url + "/databases/Northwind";

			SetupReplication(store1.DatabaseCommands, db2Url);

			var replicationDocument = store1.DatabaseCommands.Get("Raven/Replication/Destinations");

			var request = store1.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null,
																							db1Url + "/admin/replicationInfo",
																							"POST", CredentialCache.DefaultCredentials, store1.Conventions));

			request.Write(replicationDocument.DataAsJson.ToString(Formatting.None));
			var result = request.ReadResponseJson() as RavenJArray;

			Assert.NotNull(result);
			Assert.Equal(1, result.Length);
			Assert.Equal("Replication Bundle not activated.", result[0].Value<string>("Status"));
			Assert.Equal(400, result[0].Value<int>("Code"));
		}

		[Fact]
		public void DestinationServerDownTest()
		{
			server1 = CreateServer(8111, "D1");

			store1 = new DocumentStore
			{
				DefaultDatabase = "Northwind",
				Url = "http://localhost:8111"
			};

			store1.Initialize();

			store1.DatabaseCommands.GlobalAdmin.CreateDatabase(
				new DatabaseDocument
				{
					Id = "Northwind",
					Settings = { { "Raven/ActiveBundles", "replication" }, { "Raven/DataDir", @"~\D1\N" } }
				});

			var db1Url = store1.Url + "/databases/Northwind";

			SetupReplication(store1.DatabaseCommands, "http://localhost:8112/databases/Northwind");

			var replicationDocument = store1.DatabaseCommands.Get("Raven/Replication/Destinations");

			var request = store1.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null,
																							db1Url + "/admin/replicationInfo",
																							"POST", CredentialCache.DefaultCredentials, store1.Conventions));

			request.Write(replicationDocument.DataAsJson.ToString(Formatting.None));
			var result = request.ReadResponseJson() as RavenJArray;

			Assert.NotNull(result);
			Assert.Equal(1, result.Length);
			Assert.NotNull(result[0].Value<string>("Status"));
			Assert.Equal(-2, result[0].Value<int>("Code"));
		}

		[Fact]
		public void InvalidUrlTest()
		{
			server1 = CreateServer(8111, "D1");

			store1 = new DocumentStore
			{
				DefaultDatabase = "Northwind",
				Url = "http://localhost:8111"
			};

			store1.Initialize();

			store1.DatabaseCommands.GlobalAdmin.CreateDatabase(
				new DatabaseDocument
				{
					Id = "Northwind",
					Settings = { { "Raven/ActiveBundles", "replication" }, { "Raven/DataDir", @"~\D1\N" } }
				});

			var db1Url = store1.Url + "/databases/Northwind";

			SetupReplication(store1.DatabaseCommands, "http://localhost:8112/databases/Northwind");

			var request = store1.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null,
																							db1Url + "/admin/replicationInfo",
																							"POST", CredentialCache.DefaultCredentials, store1.Conventions));



			request.Write(RavenJObject.FromObject(new ReplicationDocument
			{
				Destinations = new EquatableList<ReplicationDestination>
				{
					new ReplicationDestination
					{
						Url = "http://unknown.url/"
					}
				}
			}).ToString(Formatting.None));
			var result = request.ReadResponseJson() as RavenJArray;

			Assert.NotNull(result);
			Assert.Equal(1, result.Length);
			Assert.NotNull(result[0].Value<string>("Status"));
			Assert.Equal(-1, result[0].Value<int>("Code"));
		}

		private RavenDbServer CreateServer(int port, string dataDirectory, bool removeDataDirectory = true)
		{
			Raven.Database.Server.NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(port);

			var serverConfiguration = new Raven.Database.Config.RavenConfiguration
			{
				Settings = { { "Raven/ActiveBundles", "replication" } },
                AnonymousUserAccessMode = Raven.Database.Server.AnonymousUserAccessMode.Admin,
				DataDirectory = dataDirectory,
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
				RunInMemory = false,
				Port = port,
				DefaultStorageTypeName = "esent"
			};

			if (removeDataDirectory)
				IOExtensions.DeleteDirectory(serverConfiguration.DataDirectory);

			var server = new RavenDbServer(serverConfiguration);
			serverConfiguration.PostInit();

			return server;
		}

		private void StopServer(RavenDbServer server)
		{
			server.Dispose();
		}

		private RavenDbServer StartServer(RavenDbServer server)
		{
			return this.CreateServer(server.SystemDatabase.Configuration.Port, server.SystemDatabase.Configuration.DataDirectory, false);
		}

		public override void Dispose()
		{
			if (server1 != null)
			{
				server1.Dispose();
				IOExtensions.DeleteDirectory(server1.SystemDatabase.Configuration.DataDirectory);
			}

			if (server2 != null)
			{
				server2.Dispose();
				IOExtensions.DeleteDirectory(server2.SystemDatabase.Configuration.DataDirectory);
			}

			if (store1 != null)
			{
				store1.Dispose();
			}

			if (store2 != null)
			{
				store2.Dispose();
			}
		}
	}
}