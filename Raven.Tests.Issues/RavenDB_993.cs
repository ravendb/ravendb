// -----------------------------------------------------------------------
//  <copyright file="RavenDB_993.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Net;

using Lucene.Net.Support;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Connection;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	using Raven.Abstractions.Connection;

	public class RavenDB_993 : ReplicationBase
	{
		[Fact]
		public void ValidReplicationSetupTest()
		{
			var path1 = NewDataPath();
			var path2 = NewDataPath();

			var databasePath1 = Path.Combine(path1, "Northwind");
			var databasePath2 = Path.Combine(path2, "Northwind");

			using (var server1 = GetNewServer(8079, dataDirectory: path1, runInMemory: false, activeBundles: "replication"))
			using (var server2 = GetNewServer(8078, dataDirectory: path2, runInMemory: false, activeBundles: "replication"))
			using (var store1 = NewRemoteDocumentStore(false, server1, databaseName: "Northwind", runInMemory: false, ensureDatabaseExists: false))
			using (var store2 = NewRemoteDocumentStore(false, server2, databaseName: "Northwind", runInMemory: false, ensureDatabaseExists: false))
			{
				store1.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
																   {
																	   Id = "Northwind",
																	   Settings =
																	   {
																		   { "Raven/ActiveBundles", "replication" }, 
																		   { "Raven/DataDir", databasePath1 }
																	   }
																   });

				store2.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
																   {
																	   Id = "Northwind",
																	   Settings =
																	   {
																		   { "Raven/ActiveBundles", "replication" }, 
																		   { "Raven/DataDir", databasePath2 }
																	   }
																   });

				var db1Url = store1.Url + "/databases/Northwind";
				var db2Url = store2.Url + "/databases/Northwind";

				SetupReplication(store1.DatabaseCommands, db2Url);

				var replicationDocument = store1.DatabaseCommands.Get("Raven/Replication/Destinations");

				var request = store1.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, db1Url + "/admin/replicationInfo", "POST", new OperationCredentials(null, CredentialCache.DefaultCredentials), store1.Conventions));

				request.WriteAsync(replicationDocument.DataAsJson.ToString(Formatting.None)).Wait();
				var result = request.ReadResponseJson() as RavenJArray;

				Assert.NotNull(result);
				Assert.Equal(1, result.Length);
				Assert.Equal("Valid", result[0].Value<string>("Status"));
				Assert.Equal(200, result[0].Value<int>("Code"));
			}
		}

		[Fact]
		public void DestinationServerWithReplicationDisabledTest()
		{
			var path1 = NewDataPath();
			var path2 = NewDataPath();

			var databasePath1 = Path.Combine(path1, "Northwind");
			var databasePath2 = Path.Combine(path2, "Northwind");

			using (var server1 = GetNewServer(8079, dataDirectory: path1, runInMemory: false, activeBundles: "replication"))
			using (var server2 = GetNewServer(8078, dataDirectory: path2, runInMemory: false, activeBundles: "replication"))
			using (var store1 = NewRemoteDocumentStore(false, server1, databaseName: "Northwind", runInMemory: false, ensureDatabaseExists: false))
			using (var store2 = NewRemoteDocumentStore(false, server2, databaseName: "Northwind", runInMemory: false, ensureDatabaseExists: false))
			{
				store1.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
				{
					Id = "Northwind",
					Settings =
																	   {
																		   { "Raven/ActiveBundles", "replication" }, 
																		   { "Raven/DataDir", databasePath1 }
																	   }
				});

				store2.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
				{
					Id = "Northwind",
					Settings =
																	   {
																		   { "Raven/ActiveBundles", "" }, 
																		   { "Raven/DataDir", databasePath2 }
																	   }
				});

				var db1Url = store1.Url + "/databases/Northwind";
				var db2Url = store2.Url + "/databases/Northwind";

				SetupReplication(store1.DatabaseCommands, db2Url);

				var replicationDocument = store1.DatabaseCommands.Get("Raven/Replication/Destinations");

				var request = store1.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, db1Url + "/admin/replicationInfo", "POST", new OperationCredentials(null, CredentialCache.DefaultCredentials), store1.Conventions));

				request.WriteAsync(replicationDocument.DataAsJson.ToString(Formatting.None)).Wait();
				var result = request.ReadResponseJson() as RavenJArray;

				Assert.NotNull(result);
				Assert.Equal(1, result.Length);
				Assert.Equal("Replication Bundle not activated.", result[0].Value<string>("Status"));
				Assert.Equal(400, result[0].Value<int>("Code"));
			}
		}

		[Fact]
		public void DestinationServerDownTest()
		{
			var path1 = NewDataPath();
			var databasePath1 = Path.Combine(path1, "Northwind");

			using (var server1 = GetNewServer(8079, dataDirectory: path1, runInMemory: false, activeBundles: "replication"))
			using (var store1 = NewRemoteDocumentStore(false, server1, databaseName: "Northwind", runInMemory: false, ensureDatabaseExists: false))
			{
				store1.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
																   {
																	   Id = "Northwind",
																	   Settings =
																	   {
																		   { "Raven/ActiveBundles", "replication" }, 
																		   { "Raven/DataDir", databasePath1 }
																	   }
																   });

				var db1Url = store1.Url + "/databases/Northwind";

				SetupReplication(store1.DatabaseCommands, "http://localhost:8078/databases/Northwind");

				var replicationDocument = store1.DatabaseCommands.Get("Raven/Replication/Destinations");

				var request = store1.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, db1Url + "/admin/replicationInfo", "POST", new OperationCredentials(null, CredentialCache.DefaultCredentials), store1.Conventions));

				request.WriteAsync(replicationDocument.DataAsJson.ToString(Formatting.None)).Wait();
				var result = request.ReadResponseJson() as RavenJArray;

				Assert.NotNull(result);
				Assert.Equal(1, result.Length);
				Assert.NotNull(result[0].Value<string>("Status"));
				Assert.Equal(-2, result[0].Value<int>("Code"));
			}
		}

		//note - this test _will_ fail if fiddler is active during the test (DNS lookup)
		[Fact]
		public void InvalidUrlTest()
		{
			var path1 = NewDataPath();
			var databasePath1 = Path.Combine(path1, "Northwind");

			using (var server1 = GetNewServer(8079, dataDirectory: path1, runInMemory: false, activeBundles: "replication"))
			using (var store1 = NewRemoteDocumentStore(false, server1, databaseName: "Northwind", runInMemory: false, ensureDatabaseExists: false))
			{
				store1.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
				                                                   {
					                                                   Id = "Northwind", 
																	   Settings =
																	   {
																		   { "Raven/ActiveBundles", "replication" }, 
																		   { "Raven/DataDir", databasePath1 }
																	   }
				                                                   });

				var db1Url = store1.Url + "/databases/Northwind";

				SetupReplication(store1.DatabaseCommands, "http://localhost:8078/databases/Northwind");

				var request = store1.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, db1Url + "/admin/replicationInfo", "POST", new OperationCredentials(null, CredentialCache.DefaultCredentials), store1.Conventions));

				request.WriteAsync(RavenJObject.FromObject(new ReplicationDocument { Destinations = new EquatableList<ReplicationDestination> { new ReplicationDestination { Url = "http://unknown.url/" } } }).ToString(Formatting.None)).Wait();
				var result = request.ReadResponseJson() as RavenJArray;

				Assert.NotNull(result);
				Assert.Equal(1, result.Length);
				Assert.NotNull(result[0].Value<string>("Status"));
				Assert.Equal(-1, result[0].Value<int>("Code"));
			}
		}
	}
}