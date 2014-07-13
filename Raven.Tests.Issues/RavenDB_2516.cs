// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1516.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;

using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Connection;
using Raven.Database.Bundles.Replication.Data;
using Raven.Database.Config;
using Raven.Database.Server;
using Raven.Database.Server.Security;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2516 : ReplicationBase
	{
		protected override void ModifyConfiguration(InMemoryRavenConfiguration serverConfiguration)
		{
			Authentication.EnableOnce();
		}

		[Fact]
		public void ReplicationSchemaDiscovererSimpleTest()
		{
			using (var store1 = CreateStore())
			using (var store2 = CreateStore())
			using (var store3 = CreateStore())
			using (var store4 = CreateStore())
			using (var store5 = CreateStore())
			{
				using (var session1 = store1.OpenSession())
				{
					session1.Store(new Person { Name = "Name1" });
					session1.SaveChanges();
				}

				RunReplication(store1, store2, TransitiveReplicationOptions.Replicate);
				RunReplication(store2, store3, TransitiveReplicationOptions.Replicate);
				RunReplication(store3, store4, TransitiveReplicationOptions.Replicate);
				RunReplication(store4, store5, TransitiveReplicationOptions.Replicate);
				RunReplication(store5, store1, TransitiveReplicationOptions.Replicate);

				WaitForDocument<Person>(store5, "people/1");

				var url = store1.Url.ForDatabase(store1.DefaultDatabase) + "/admin/replication/schema";

				var request = store1
					.JsonRequestFactory
					.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, "POST", store1.DatabaseCommands.PrimaryCredentials, store1.Conventions));

				var json = (RavenJObject)request.ReadResponseJson();
				var schema = json.Deserialize<ReplicationSchemaRootNode>(store1.Conventions);

				Assert.NotNull(schema);
				Assert.Equal(0, schema.Sources.Count);
				Assert.Equal(1, schema.Destinations.Count);

				var sources = schema.Sources;
				var destinations = schema.Destinations;

				Assert.Equal(store2.Url.ForDatabase(store2.DefaultDatabase), destinations[0].ServerUrl);
				Assert.Equal(TransitiveReplicationOptions.Replicate, destinations[0].ReplicationBehavior);
				Assert.Equal(ReplicatonNodeState.Online, destinations[0].State);

				Assert.Equal(1, schema.Destinations[0].Sources.Count);
				Assert.Equal(1, schema.Destinations[0].Destinations.Count);

				sources = schema.Destinations[0].Sources;
				destinations = schema.Destinations[0].Destinations;

				Assert.Equal(store1.Url.ForDatabase(store1.DefaultDatabase), sources[0].ServerUrl);
				Assert.NotNull(sources[0].LastAttachmentEtag);
				Assert.NotNull(sources[0].LastDocumentEtag);
				Assert.Equal(ReplicatonNodeState.Online, sources[0].State);

				Assert.Equal(store3.Url.ForDatabase(store3.DefaultDatabase), destinations[0].ServerUrl);
				Assert.Equal(TransitiveReplicationOptions.Replicate, destinations[0].ReplicationBehavior);
				Assert.Equal(ReplicatonNodeState.Online, destinations[0].State);

				Assert.Equal(1, destinations[0].Sources.Count);
				Assert.Equal(1, destinations[0].Destinations.Count);

				sources = destinations[0].Sources;
				destinations = destinations[0].Destinations;

				Assert.Equal(store2.Url.ForDatabase(store2.DefaultDatabase), sources[0].ServerUrl);
				Assert.NotNull(sources[0].LastAttachmentEtag);
				Assert.NotNull(sources[0].LastDocumentEtag);
				Assert.Equal(ReplicatonNodeState.Online, sources[0].State);

				Assert.Equal(store4.Url.ForDatabase(store4.DefaultDatabase), destinations[0].ServerUrl);
				Assert.Equal(TransitiveReplicationOptions.Replicate, destinations[0].ReplicationBehavior);
				Assert.Equal(ReplicatonNodeState.Online, destinations[0].State);

				Assert.Equal(1, destinations[0].Sources.Count);
				Assert.Equal(1, destinations[0].Destinations.Count);

				sources = destinations[0].Sources;
				destinations = destinations[0].Destinations;

				Assert.Equal(store3.Url.ForDatabase(store3.DefaultDatabase), sources[0].ServerUrl);
				Assert.NotNull(sources[0].LastAttachmentEtag);
				Assert.NotNull(sources[0].LastDocumentEtag);
				Assert.Equal(ReplicatonNodeState.Online, sources[0].State);

				Assert.Equal(store5.Url.ForDatabase(store5.DefaultDatabase), destinations[0].ServerUrl);
				Assert.Equal(TransitiveReplicationOptions.Replicate, destinations[0].ReplicationBehavior);
				Assert.Equal(ReplicatonNodeState.Online, destinations[0].State);

				Assert.Equal(1, destinations[0].Sources.Count);
				Assert.Equal(1, destinations[0].Destinations.Count);

				sources = destinations[0].Sources;
				destinations = destinations[0].Destinations;

				Assert.Equal(store4.Url.ForDatabase(store4.DefaultDatabase), sources[0].ServerUrl);
				Assert.NotNull(sources[0].LastAttachmentEtag);
				Assert.NotNull(sources[0].LastDocumentEtag);
				Assert.Equal(ReplicatonNodeState.Online, sources[0].State);

				Assert.Equal(store1.Url.ForDatabase(store1.DefaultDatabase), destinations[0].ServerUrl);
				Assert.Equal(TransitiveReplicationOptions.Replicate, destinations[0].ReplicationBehavior);
				Assert.Equal(ReplicatonNodeState.Online, destinations[0].State);
			}
		}

		[Fact]
		public void ReplicationSchemaDiscovererSimpleTestWithOAuth()
		{
			using (var store1 = CreateStore(enableAuthorization: true, anonymousUserAccessMode: AnonymousUserAccessMode.None, configureStore: store => store.ApiKey = "Ayende/abc"))
			using (var store2 = CreateStore(enableAuthorization: true, anonymousUserAccessMode: AnonymousUserAccessMode.None, configureStore: store => store.ApiKey = "Ayende/abc"))
			using (var store3 = CreateStore(enableAuthorization: true, anonymousUserAccessMode: AnonymousUserAccessMode.None, configureStore: store => store.ApiKey = "Ayende/abc"))
			using (var store4 = CreateStore(enableAuthorization: true, anonymousUserAccessMode: AnonymousUserAccessMode.None, configureStore: store => store.ApiKey = "Ayende/abc"))
			using (var store5 = CreateStore(enableAuthorization: true, anonymousUserAccessMode: AnonymousUserAccessMode.None, configureStore: store => store.ApiKey = "Ayende/abc"))
			{
				foreach (var server in servers)
				{
					server.SystemDatabase.Documents.Put("Raven/ApiKeys/Ayende", null, RavenJObject.FromObject(new ApiKeyDefinition
					{
						Databases = new List<ResourceAccess>
						            {
							            new ResourceAccess { TenantId = "*", Admin = true }, 
										new ResourceAccess { TenantId = "<system>", Admin = true },
						            },
						Enabled = true,
						Name = "Ayende",
						Secret = "abc"
					}), new RavenJObject(), null);
				}

				using (var session1 = store1.OpenSession())
				{
					session1.Store(new Person { Name = "Name1" });
					session1.SaveChanges();
				}

				RunReplication(store1, store2, TransitiveReplicationOptions.Replicate, apiKey: "Ayende/abc");
				RunReplication(store2, store3, TransitiveReplicationOptions.Replicate, apiKey: "Ayende/abc");
				RunReplication(store3, store4, TransitiveReplicationOptions.Replicate, apiKey: "Ayende/abc");
				RunReplication(store4, store5, TransitiveReplicationOptions.Replicate, apiKey: "Ayende/abc");
				RunReplication(store5, store1, TransitiveReplicationOptions.Replicate, apiKey: "Ayende/abc");

				WaitForDocument<Person>(store5, "people/1");

				var url = store1.Url.ForDatabase(store1.DefaultDatabase) + "/admin/replication/schema";

				var request = store1
					.JsonRequestFactory
					.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, "POST", store1.DatabaseCommands.PrimaryCredentials, store1.Conventions));

				var json = (RavenJObject)request.ReadResponseJson();
				var schema = json.Deserialize<ReplicationSchemaRootNode>(store1.Conventions);

				Assert.NotNull(schema);
				Assert.Equal(0, schema.Sources.Count);
				Assert.Equal(1, schema.Destinations.Count);

				var sources = schema.Sources;
				var destinations = schema.Destinations;

				Assert.Equal(store2.Url.ForDatabase(store2.DefaultDatabase), destinations[0].ServerUrl);
				Assert.Equal(TransitiveReplicationOptions.Replicate, destinations[0].ReplicationBehavior);
				Assert.Equal(ReplicatonNodeState.Online, destinations[0].State);

				Assert.Equal(1, schema.Destinations[0].Sources.Count);
				Assert.Equal(1, schema.Destinations[0].Destinations.Count);

				sources = schema.Destinations[0].Sources;
				destinations = schema.Destinations[0].Destinations;

				Assert.Equal(store1.Url.ForDatabase(store1.DefaultDatabase), sources[0].ServerUrl);
				Assert.NotNull(sources[0].LastAttachmentEtag);
				Assert.NotNull(sources[0].LastDocumentEtag);
				Assert.Equal(ReplicatonNodeState.Online, sources[0].State);

				Assert.Equal(store3.Url.ForDatabase(store3.DefaultDatabase), destinations[0].ServerUrl);
				Assert.Equal(TransitiveReplicationOptions.Replicate, destinations[0].ReplicationBehavior);
				Assert.Equal(ReplicatonNodeState.Online, destinations[0].State);

				Assert.Equal(1, destinations[0].Sources.Count);
				Assert.Equal(1, destinations[0].Destinations.Count);

				sources = destinations[0].Sources;
				destinations = destinations[0].Destinations;

				Assert.Equal(store2.Url.ForDatabase(store2.DefaultDatabase), sources[0].ServerUrl);
				Assert.NotNull(sources[0].LastAttachmentEtag);
				Assert.NotNull(sources[0].LastDocumentEtag);
				Assert.Equal(ReplicatonNodeState.Online, sources[0].State);

				Assert.Equal(store4.Url.ForDatabase(store4.DefaultDatabase), destinations[0].ServerUrl);
				Assert.Equal(TransitiveReplicationOptions.Replicate, destinations[0].ReplicationBehavior);
				Assert.Equal(ReplicatonNodeState.Online, destinations[0].State);

				Assert.Equal(1, destinations[0].Sources.Count);
				Assert.Equal(1, destinations[0].Destinations.Count);

				sources = destinations[0].Sources;
				destinations = destinations[0].Destinations;

				Assert.Equal(store3.Url.ForDatabase(store3.DefaultDatabase), sources[0].ServerUrl);
				Assert.NotNull(sources[0].LastAttachmentEtag);
				Assert.NotNull(sources[0].LastDocumentEtag);
				Assert.Equal(ReplicatonNodeState.Online, sources[0].State);

				Assert.Equal(store5.Url.ForDatabase(store5.DefaultDatabase), destinations[0].ServerUrl);
				Assert.Equal(TransitiveReplicationOptions.Replicate, destinations[0].ReplicationBehavior);
				Assert.Equal(ReplicatonNodeState.Online, destinations[0].State);

				Assert.Equal(1, destinations[0].Sources.Count);
				Assert.Equal(1, destinations[0].Destinations.Count);

				sources = destinations[0].Sources;
				destinations = destinations[0].Destinations;

				Assert.Equal(store4.Url.ForDatabase(store4.DefaultDatabase), sources[0].ServerUrl);
				Assert.NotNull(sources[0].LastAttachmentEtag);
				Assert.NotNull(sources[0].LastDocumentEtag);
				Assert.Equal(ReplicatonNodeState.Online, sources[0].State);

				Assert.Equal(store1.Url.ForDatabase(store1.DefaultDatabase), destinations[0].ServerUrl);
				Assert.Equal(TransitiveReplicationOptions.Replicate, destinations[0].ReplicationBehavior);
				Assert.Equal(ReplicatonNodeState.Online, destinations[0].State);
			}
		}
	}
}