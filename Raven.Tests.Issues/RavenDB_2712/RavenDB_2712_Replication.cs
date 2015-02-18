// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2712.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Extensions;
using Raven.Database.Config.Retriever;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues.RavenDB_2712
{
	public class RavenDB_2712_Replication : ReplicationBase
	{
		public RavenDB_2712_Replication()
		{
			ConfigurationRetriever.EnableGlobalConfigurationOnce();
		}

		[Fact]
		public void IfThereIsNoLocalConfigurationThenGlobalShouldBeUsed()
		{
			using (NewRemoteDocumentStore(databaseName: "Northwind"))
			{
				var server = servers[0];
				var systemDatabase = server.SystemDatabase;
				var database = server.Server.GetDatabaseInternal("Northwind").ResultUnwrap();
				var retriever = database.ConfigurationRetriever;
				var document = retriever.GetConfigurationDocument<ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin>>(Constants.RavenReplicationDestinations);

				Assert.Null(document);

				systemDatabase
					.Documents
					.Put(
						Constants.Global.ReplicationDestinationsDocumentName,
						null,
						RavenJObject.FromObject(new ReplicationDocument
												{
													Id = Constants.Global.ReplicationDestinationsDocumentName,
													Destinations =
							                        {
								                        new ReplicationDestination
								                        {
									                        ApiKey = "key1", 
									                        Database = "db1", 
									                        ClientVisibleUrl = "curl1", 
									                        Disabled = true, 
									                        Domain = "d1", 
									                        IgnoredClient = true, 
									                        Password = "p1", 
									                        SkipIndexReplication = false, 
									                        TransitiveReplicationBehavior = TransitiveReplicationOptions.Replicate, 
									                        Url = "http://localhost:8080", 
									                        Username = "u1"
								                        }
							                        }
												}), new RavenJObject(), null);

				document = retriever.GetConfigurationDocument<ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin>>(Constants.RavenReplicationDestinations);

				Assert.NotNull(document);
				Assert.True(document.GlobalExists);
				Assert.False(document.LocalExists);
                Assert.Equal(Constants.RavenReplicationDestinations, document.MergedDocument.Id);
                Assert.Equal(database.TransactionalStorage.Id.ToString(), document.MergedDocument.Source);
                Assert.Equal(1, document.MergedDocument.Destinations.Count);

                var destination = document.MergedDocument.Destinations[0];

				Assert.True(destination.HasGlobal);
				Assert.False(destination.HasLocal);
				Assert.Equal("key1", destination.ApiKey);
				Assert.Equal("Northwind", destination.Database);
				Assert.Equal("curl1", destination.ClientVisibleUrl);
				Assert.True(destination.Disabled);
				Assert.Equal("d1", destination.Domain);
				Assert.True(destination.IgnoredClient);
				Assert.Equal("p1", destination.Password);
				Assert.False(destination.SkipIndexReplication);
				Assert.Equal(TransitiveReplicationOptions.Replicate, destination.TransitiveReplicationBehavior);
				Assert.Equal("http://localhost:8080", destination.Url);
				Assert.Equal("u1", destination.Username);
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

				database
					.Documents
					.Put(
						Constants.RavenReplicationDestinations,
						null,
						RavenJObject.FromObject(new ReplicationDocument
												{
													Id = Constants.RavenReplicationDestinations,
													Destinations = {
								                                       new ReplicationDestination
								                                       {
									                                       ApiKey = "key2", 
									                                       Database = "Northwind", 
									                                       ClientVisibleUrl = "curl2", 
									                                       Disabled = false, 
									                                       Domain = "d2", 
									                                       IgnoredClient = false, 
									                                       Password = "p2", 
									                                       SkipIndexReplication = true, 
									                                       TransitiveReplicationBehavior = TransitiveReplicationOptions.None, 
									                                       Url = "http://localhost:8080", 
									                                       Username = "u2"
								                                       }
							                                       },
													Source = database.TransactionalStorage.Id.ToString()
												}), new RavenJObject(), null);

				systemDatabase
					.Documents
					.Put(
						Constants.Global.ReplicationDestinationsDocumentName,
						null,
						RavenJObject.FromObject(new ReplicationDocument
												{
													Id = Constants.Global.ReplicationDestinationsDocumentName,
													Destinations = {
								                                       new ReplicationDestination
								                                       {
									                                       ApiKey = "key1", 
									                                       Database = "db1", 
									                                       ClientVisibleUrl = "curl1", 
									                                       Disabled = true, 
									                                       Domain = "d1", 
									                                       IgnoredClient = true, 
									                                       Password = "p1", 
									                                       SkipIndexReplication = false, 
									                                       TransitiveReplicationBehavior = TransitiveReplicationOptions.Replicate, 
									                                       Url = "http://localhost:8080", 
									                                       Username = "u1"
								                                       }
							                                       },
													Source = systemDatabase.TransactionalStorage.Id.ToString()
												}), new RavenJObject(), null);

				var document = retriever.GetConfigurationDocument<ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin>>(Constants.RavenReplicationDestinations);

				Assert.NotNull(document);
				Assert.True(document.GlobalExists);
				Assert.True(document.LocalExists);
                Assert.Equal(Constants.RavenReplicationDestinations, document.MergedDocument.Id);
                Assert.Equal(database.TransactionalStorage.Id.ToString(), document.MergedDocument.Source);
                Assert.Equal(1, document.MergedDocument.Destinations.Count);

                var destination = document.MergedDocument.Destinations[0];

				Assert.True(destination.HasGlobal);
				Assert.True(destination.HasLocal);
				Assert.Equal("key2", destination.ApiKey);
				Assert.Equal("Northwind", destination.Database);
				Assert.Equal("curl2", destination.ClientVisibleUrl);
				Assert.False(destination.Disabled);
				Assert.Equal("d2", destination.Domain);
				Assert.False(destination.IgnoredClient);
				Assert.Equal("p2", destination.Password);
				Assert.True(destination.SkipIndexReplication);
				Assert.Equal(TransitiveReplicationOptions.None, destination.TransitiveReplicationBehavior);
				Assert.Equal("http://localhost:8080", destination.Url);
				Assert.Equal("u2", destination.Username);
			}
		}

		[Fact]
		public void GlobalConfigurationWillBeAppliedToLocalButWithoutOverwrites()
		{
			using (NewRemoteDocumentStore(databaseName: "Northwind"))
			{
				var server = servers[0];
				var systemDatabase = server.SystemDatabase;
				var database = server.Server.GetDatabaseInternal("Northwind").ResultUnwrap();
				var retriever = database.ConfigurationRetriever;

				database
					.Documents
					.Put(
						Constants.RavenReplicationDestinations,
						null,
						RavenJObject.FromObject(new ReplicationDocument
												{
													Id = Constants.RavenReplicationDestinations,
													Destinations = {
								                                       new ReplicationDestination
								                                       {
									                                       ApiKey = "key2", 
									                                       Database = "db2", 
									                                       ClientVisibleUrl = "curl2", 
									                                       Disabled = false, 
									                                       Domain = "d2", 
									                                       IgnoredClient = false, 
									                                       Password = "p2", 
									                                       SkipIndexReplication = true, 
									                                       TransitiveReplicationBehavior = TransitiveReplicationOptions.None, 
									                                       Url = "http://localhost:8080", 
									                                       Username = "u2"
								                                       }
							                                       },
													Source = database.TransactionalStorage.Id.ToString()
												}), new RavenJObject(), null);

				systemDatabase
					.Documents
					.Put(
						Constants.Global.ReplicationDestinationsDocumentName,
						null,
						RavenJObject.FromObject(new ReplicationDocument
												{
													Id = Constants.Global.ReplicationDestinationsDocumentName,
													Destinations = {
								                                       new ReplicationDestination
								                                       {
									                                       ApiKey = "key1", 
									                                       Database = "db1", 
									                                       ClientVisibleUrl = "curl1", 
									                                       Disabled = true, 
									                                       Domain = "d1", 
									                                       IgnoredClient = true, 
									                                       Password = "p1", 
									                                       SkipIndexReplication = false, 
									                                       TransitiveReplicationBehavior = TransitiveReplicationOptions.Replicate, 
									                                       Url = "http://localhost:8080", 
									                                       Username = "u1"
								                                       }
							                                       },
													Source = systemDatabase.TransactionalStorage.Id.ToString()
												}), new RavenJObject(), null);

				var document = retriever.GetConfigurationDocument<ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin>>(Constants.RavenReplicationDestinations);

				Assert.NotNull(document);
				Assert.True(document.GlobalExists);
				Assert.True(document.LocalExists);
                Assert.Equal(Constants.RavenReplicationDestinations, document.MergedDocument.Id);
                Assert.Equal(database.TransactionalStorage.Id.ToString(), document.MergedDocument.Source);
                Assert.Equal(2, document.MergedDocument.Destinations.Count);

                var destination = document.MergedDocument.Destinations[0];

				Assert.False(destination.HasGlobal);
				Assert.True(destination.HasLocal);
				Assert.Equal("key2", destination.ApiKey);
				Assert.Equal("db2", destination.Database);
				Assert.Equal("curl2", destination.ClientVisibleUrl);
				Assert.False(destination.Disabled);
				Assert.Equal("d2", destination.Domain);
				Assert.False(destination.IgnoredClient);
				Assert.Equal("p2", destination.Password);
				Assert.True(destination.SkipIndexReplication);
				Assert.Equal(TransitiveReplicationOptions.None, destination.TransitiveReplicationBehavior);
				Assert.Equal("http://localhost:8080", destination.Url);
				Assert.Equal("u2", destination.Username);

                destination = document.MergedDocument.Destinations[1];

				Assert.True(destination.HasGlobal);
				Assert.False(destination.HasLocal);
				Assert.Equal("key1", destination.ApiKey);
				Assert.Equal("Northwind", destination.Database);
				Assert.Equal("curl1", destination.ClientVisibleUrl);
				Assert.True(destination.Disabled);
				Assert.Equal("d1", destination.Domain);
				Assert.True(destination.IgnoredClient);
				Assert.Equal("p1", destination.Password);
				Assert.False(destination.SkipIndexReplication);
				Assert.Equal(TransitiveReplicationOptions.Replicate, destination.TransitiveReplicationBehavior);
				Assert.Equal("http://localhost:8080", destination.Url);
				Assert.Equal("u1", destination.Username);
			}
		}
	}
}