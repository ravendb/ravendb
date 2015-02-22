// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2712_SqlReplication.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;

using Raven.Abstractions.Data;
using Raven.Client.Extensions;
using Raven.Database.Bundles.SqlReplication;
using Raven.Json.Linq;

using Xunit;

namespace Raven.Tests.Issues.RavenDB_2712
{
	public class RavenDB_2712_SqlReplication : GlobalConfigurationTest
	{
		[Fact]
		public void GlobalDefaultConfigurationShouldBeEffectiveIfThereIsNoLocal()
		{
			using (NewRemoteDocumentStore(databaseName: "Northwind"))
			{
				var server = servers[0];
				var systemDatabase = server.SystemDatabase;
				var database = server.Server.GetDatabaseInternal("Northwind").ResultUnwrap();
				var retriever = database.ConfigurationRetriever;

				var document = retriever.GetConfigurationDocument<SqlReplicationConnections<SqlReplicationConnections.PredefinedSqlConnectionWithConfigurationOrigin>>(Constants.SqlReplication.SqlReplicationConnectionsDocumentName);

				Assert.Null(document);

				systemDatabase
					.Documents
					.Put(
						Constants.Global.SqlReplicationConnectionsDocumentName,
						null,
						RavenJObject.FromObject(new SqlReplicationConnections
						{
							PredefinedConnections = new List<SqlReplicationConnections.PredefinedSqlConnection>
							                        {
								                        new SqlReplicationConnections.PredefinedSqlConnection {ConnectionString = "cs1", FactoryName = "f1", Name = "n1"} 
													}
						}),
						new RavenJObject(),
						null);

				document = retriever.GetConfigurationDocument<SqlReplicationConnections<SqlReplicationConnections.PredefinedSqlConnectionWithConfigurationOrigin>>(Constants.SqlReplication.SqlReplicationConnectionsDocumentName);

				Assert.NotNull(document);
				Assert.True(document.GlobalExists);
				Assert.False(document.LocalExists);
                Assert.Equal(1, document.MergedDocument.PredefinedConnections.Count);

                var connection = document.MergedDocument.PredefinedConnections[0];

				Assert.True(connection.HasGlobal);
				Assert.False(connection.HasLocal);
				Assert.Equal("cs1", connection.ConnectionString);
				Assert.Equal("f1", connection.FactoryName);
				Assert.Equal("n1", connection.Name);
			}
		}

		[Fact]
		public void LocalConfigurationTakesPrecedenceBeforeGlobal()
		{
			using (NewRemoteDocumentStore(databaseName: "Northwind"))
			{
				var server = servers[0];
				var systemDatabase = server.SystemDatabase;
				var database = server.Server.GetDatabaseInternal("Northwind").ResultUnwrap();
				var retriever = database.ConfigurationRetriever;

				var document = retriever.GetConfigurationDocument<SqlReplicationConnections<SqlReplicationConnections.PredefinedSqlConnectionWithConfigurationOrigin>>(Constants.SqlReplication.SqlReplicationConnectionsDocumentName);

				Assert.Null(document);

				systemDatabase
					.Documents
					.Put(
						Constants.Global.SqlReplicationConnectionsDocumentName,
						null,
						RavenJObject.FromObject(new SqlReplicationConnections
						{
							PredefinedConnections = new List<SqlReplicationConnections.PredefinedSqlConnection>
							                        {
								                        new SqlReplicationConnections.PredefinedSqlConnection {ConnectionString = "cs1", FactoryName = "f1", Name = "n1"},
														new SqlReplicationConnections.PredefinedSqlConnection {ConnectionString = "cs2", FactoryName = "f2", Name = "n2"}
													}
						}),
						new RavenJObject(),
						null);

				database
					.Documents
					.Put(
						Constants.SqlReplication.SqlReplicationConnectionsDocumentName,
						null,
						RavenJObject.FromObject(new SqlReplicationConnections
						{
							PredefinedConnections = new List<SqlReplicationConnections.PredefinedSqlConnection>
							                        {
								                        new SqlReplicationConnections.PredefinedSqlConnection {ConnectionString = "cs", FactoryName = "f", Name = "n1"},
														new SqlReplicationConnections.PredefinedSqlConnection {ConnectionString = "cs3", FactoryName = "f3", Name = "n3"}
													}
						}),
						new RavenJObject(),
						null);

				document = retriever.GetConfigurationDocument<SqlReplicationConnections<SqlReplicationConnections.PredefinedSqlConnectionWithConfigurationOrigin>>(Constants.SqlReplication.SqlReplicationConnectionsDocumentName);

				Assert.NotNull(document);
				Assert.True(document.GlobalExists);
				Assert.True(document.LocalExists);

                Assert.Equal(3, document.MergedDocument.PredefinedConnections.Count);

                var connection = document.MergedDocument.PredefinedConnections.First(x => x.Name == "n1");

				Assert.True(connection.HasGlobal);
				Assert.True(connection.HasLocal);
				Assert.Equal("n1", connection.Name);
				Assert.Equal("f", connection.FactoryName);
				Assert.Equal("cs", connection.ConnectionString);

                connection = document.MergedDocument.PredefinedConnections.First(x => x.Name == "n2");

				Assert.True(connection.HasGlobal);
				Assert.False(connection.HasLocal);
				Assert.Equal("n2", connection.Name);
				Assert.Equal("f2", connection.FactoryName);
				Assert.Equal("cs2", connection.ConnectionString);

                connection = document.MergedDocument.PredefinedConnections.First(x => x.Name == "n3");

				Assert.False(connection.HasGlobal);
				Assert.True(connection.HasLocal);
				Assert.Equal("n3", connection.Name);
				Assert.Equal("f3", connection.FactoryName);
				Assert.Equal("cs3", connection.ConnectionString);
			}
		}
	}
}