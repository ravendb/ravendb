using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.DataArchival;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Refresh;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.SchemaValidation;
using Raven.Client.Exceptions.SchemaValidation;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using SVC = Raven.Server.Documents.SchemaValidation.SchemaValidatorConstants;

namespace SlowTests.SchemaValidation;

public class SchemaValidationFeaturesTests : ReplicationTestBase
{
    public SchemaValidationFeaturesTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Replication)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task RejectDocumentFromExternalReplication(Options options)
    {
        using (var store1 = GetDocumentStore(new Options(options)
               {
                   ModifyDatabaseName = s => $"{s}_FooBar-1"
               }))
        using (var store2 = GetDocumentStore(new Options(options)
               {
                   ModifyDatabaseName = s => $"{s}_FooBar-2"
               }))
        {
            await SetupReplicationAsync(store1, store2);

            string schemaDefinition;
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                schemaDefinition =
                    context.ReadObject(
                        new DynamicJsonValue { [SVC.Properties] = new DynamicJsonValue { ["Prop"] = new DynamicJsonValue { [SVC.MaxLength] = 10 } } },
                        "schema-validation-configuration").ToString();
            }

            await store2.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(new SchemaValidationConfiguration
            {
                ValidatorsPerCollection = new Dictionary<string, SchemaDefinition>
                {
                    {"TestObjs", new SchemaDefinition
                    {
                        Schema = schemaDefinition
                    }}
                }
            }));

            const string invalidDocId = "invalidDocId";

            using (var s2 = store2.OpenAsyncSession())
            {
                await s2.StoreAsync(new TestObj { Prop = "0123456789a" }, invalidDocId);
                var error = await Assert.ThrowsAsync<SchemaValidationException>(async () => await s2.SaveChangesAsync());
                Assert.Contains("The length of the value '0123456789a' at 'Prop' should not exceed 10, but its actual length is 11.", error.Message);
            }

            using (var s1 = store1.OpenAsyncSession())
            {
                await s1.StoreAsync(new TestObj { Prop = "0123456789a" }, invalidDocId);
                await s1.SaveChangesAsync();
            }

            var count = await WaitForValueAsync(async () =>
            {
                var replicationFailureInfo = await store1.Maintenance.SendAsync(new GetReplicationOutgoingsFailureInfoOperation(nodeTag: "A", shardNumber: 0));
                return replicationFailureInfo.Stats.Count;
            }, 1);

            Assert.Equal(1, count);

            var replicationFailureInfo = await store1.Maintenance.SendAsync(new GetReplicationOutgoingsFailureInfoOperation(nodeTag: "A", shardNumber: 0));
            var info = replicationFailureInfo.Stats.Single();
            Assert.True(info.Key is ExternalReplication);

            using (var s2 = store2.OpenAsyncSession())
            {
                var loaded = await s2.LoadAsync<TestObj>(invalidDocId);
                Assert.Null(loaded);
            }
        }
    }


    [RavenFact(RavenTestCategory.ExpirationRefresh)]
    public async Task RefreshShouldSkipSchemaValidation()
    {
        using (var store = GetDocumentStore())
        {
            var config = new RefreshConfiguration
            {
                Disabled = false,
                RefreshFrequencyInSec = 100,
            };

            var result = await store.Maintenance.SendAsync(new ConfigureRefreshOperation(config));
            await Server.ServerStore.Cluster.WaitForIndexNotification(result.RaftCommandIndex ?? 1, TimeSpan.FromMinutes(1));

            string expectedChangeVector;
            using (var session = store.OpenAsyncSession())
            {
                var user = new User { Name = "Grisha" };
                await session.StoreAsync(user, "users/1-A");
                session.Advanced.GetMetadataFor(user)["@refresh"] = DateTime.UtcNow.AddHours(-1).ToString("o");
                await session.SaveChangesAsync();

                expectedChangeVector = session.Advanced.GetChangeVectorFor(user);
            }

            await SetupSchemaValidation(store);

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            var expiredDocumentsCleaner = database.ExpiredDocumentsCleaner;
            await expiredDocumentsCleaner.RefreshDocs(throwOnError: true);

            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>("users/1-A");
                Assert.NotNull(user);
                var actualChangeVector = session.Advanced.GetChangeVectorFor(user);

                Assert.NotEqual(expectedChangeVector, actualChangeVector);
            }

        }
    }

    [RavenFact(RavenTestCategory.ExpirationRefresh)]
    public async Task DataArchivalShouldSkipSchemaValidation()
    {
        using (var store = GetDocumentStore())
        {
            var expiry = SystemTime.UtcNow.AddMinutes(5);
            var metadata = new Dictionary<string, object>
            {
                [Constants.Documents.Metadata.ArchiveAt] = expiry.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite)
            };

            using (var session = store.OpenAsyncSession())
            {
                var user = new User { Name = "Grisha" };
                await session.StoreAsync(user);
                var metadataFromDoc = session.Advanced.GetMetadataFor(user);
                metadataFromDoc[Constants.Documents.Metadata.ArchiveAt] = metadata[Constants.Documents.Metadata.ArchiveAt];
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var count = await session.Query<User>().Where(x => x.Name == "Grisha").CountAsync();
                Assert.Equal(1, count);
            }

            await SetupSchemaValidation(store);

            var config = new DataArchivalConfiguration { Disabled = false, ArchiveFrequencyInSec = 100 };
            await DataArchivalHelper.SetupDataArchival(store, Server.ServerStore, config);

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            var documentsArchiver = database.DataArchivist;
            await documentsArchiver.ArchiveDocs();

            await Indexes.WaitForIndexingAsync(store);

            using (var session = store.OpenAsyncSession())
            {
                var count = await session.Query<User>().Where(x => x.Name == "Grisha").CountAsync();
                Assert.Equal(0, count);
            }
        }
    }

    private static async Task SetupSchemaValidation(DocumentStore store)
    {
        string schemaDefinition;
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            schemaDefinition =
                context.ReadObject(
                    new DynamicJsonValue { [SVC.Properties] = new DynamicJsonValue { ["Name"] = new DynamicJsonValue { [SVC.MaxLength] = 1 } } },
                    "schema-validation-configuration").ToString();
        }

        await store.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(new SchemaValidationConfiguration
        {
            ValidatorsPerCollection = new Dictionary<string, SchemaDefinition>
            {
                {"Users", new SchemaDefinition
                {
                    Schema = schemaDefinition
                }}
            }
        }));
    }

    private class TestObj
    {
        public string Id { get; set; }
        public string Prop { get; set; }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task PullReplication_HubToSink_SchemaOnSink_InvalidDocDoesNotBreakReplication()
    {
        var name = $"pull-replication {GetDatabaseName()}";

        var settings = new Dictionary<string, string>
        {
            [RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = "10",
            [RavenConfiguration.GetKey(x => x.Databases.FrequencyToCheckForIdle)] = "3",
            [RavenConfiguration.GetKey(x => x.Replication.RetryMaxTimeout)] = "1",
            [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false"
        };

        var certificates = Certificates.SetupServerAuthentication(customSettings: settings);

        using (var server = GetNewServer(new ServerCreationOptions { CustomSettings = settings }))
        using (var sink = GetDocumentStore(new Options
        {
            Server = server,
            ModifyDatabaseName = s => $"Sink_{s}",
            RunInMemory = false,
            ClientCertificate = certificates.ServerCertificateForCommunication.Value,
            AdminCertificate = certificates.ServerCertificateForCommunication.Value
        }))
        using (var hub = GetDocumentStore(new Options
        {
            Server = server,
            ModifyDatabaseName = s => $"Hub_{s}",
            RunInMemory = false,
            ClientCertificate = certificates.ServerCertificateForCommunication.Value,
            AdminCertificate = certificates.ServerCertificateForCommunication.Value
        }))
        {
            // Define schema ONLY on SINK (destination of Hub->Sink)
            await ConfigureCategoriesSchemaAsync(sink);

            await hub.Maintenance.ForDatabase(hub.Database).SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
            {
                Name = name,
                Mode = PullReplicationMode.HubToSink
            }));

            await hub.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = name,
                    CertificateBase64 = Convert.ToBase64String(certificates.ClientCertificate1.Value.Export(X509ContentType.Cert)),
                }));

            var conStrName = "PullReplicationAsSink";
            await sink.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Database = hub.Database,
                Name = conStrName,
                TopologyDiscoveryUrls = hub.Urls
            }));

            await sink.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
            {
                ConnectionStringName = conStrName,
                CertificateWithPrivateKey = Convert.ToBase64String(certificates.ClientCertificate1.Value.Export(X509ContentType.Pfx)),
                HubName = name,
                Mode = PullReplicationMode.HubToSink
            }));

            // 1) Valid doc on HUB should replicate to SINK
            StoreCategory(hub, "categories/1-A", "Valid #1", "ok");

            // 2) Invalid doc on HUB should be rejected on SINK (schema), but must NOT break replication
            StoreInvalidCategoryWithExtraField(hub, "categories/2-A");

            // Ensure the invalid one did NOT replicate
            using (var session = sink.OpenSession())
            {
                var doc = session.Load<Category>("categories/2-A");
                Assert.True(doc == null, "Invalid document should not be accepted by schema on sink.");
            }

            var replicationFailureInfo = await sink.Maintenance.SendAsync(new GetReplicationOutgoingsFailureInfoOperation(nodeTag: server.ServerStore.NodeTag));
            var info = replicationFailureInfo.Stats.Single();
            Assert.True(info.Key is PullReplicationAsSink);
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task PullReplication_SinkToHub_SchemaOnHub_InvalidDocDoesNotBreakReplication()
    {
        var name = $"pull-replication {GetDatabaseName()}";

        var settings = new Dictionary<string, string>
        {
            [RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = "10",
            [RavenConfiguration.GetKey(x => x.Databases.FrequencyToCheckForIdle)] = "3",
            [RavenConfiguration.GetKey(x => x.Replication.RetryMaxTimeout)] = "1",
            [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false"
        };

        var certificates = Certificates.SetupServerAuthentication(customSettings: settings);

        using (var server = GetNewServer(new ServerCreationOptions { CustomSettings = settings }))
        using (var sink = GetDocumentStore(new Options
        {
            Server = server,
            ModifyDatabaseName = s => $"Sink_{s}",
            RunInMemory = false,
            ClientCertificate = certificates.ServerCertificateForCommunication.Value,
            AdminCertificate = certificates.ServerCertificateForCommunication.Value
        }))
        using (var hub = GetDocumentStore(new Options
        {
            Server = server,
            ModifyDatabaseName = s => $"Hub_{s}",
            RunInMemory = false,
            ClientCertificate = certificates.ServerCertificateForCommunication.Value,
            AdminCertificate = certificates.ServerCertificateForCommunication.Value
        }))
        {
            // Define schema ONLY on HUB (destination of Sink->Hub)
            await ConfigureCategoriesSchemaAsync(hub);

            await hub.Maintenance.ForDatabase(hub.Database).SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
            {
                Name = name,
                Mode = PullReplicationMode.SinkToHub
            }));

            await hub.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = name,
                    CertificateBase64 = Convert.ToBase64String(certificates.ClientCertificate1.Value.Export(X509ContentType.Cert)),
                }));

            var conStrName = "PullReplicationAsSink";
            await sink.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Database = hub.Database,
                Name = conStrName,
                TopologyDiscoveryUrls = hub.Urls
            }));

            await sink.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
            {
                ConnectionStringName = conStrName,
                CertificateWithPrivateKey = Convert.ToBase64String(certificates.ClientCertificate1.Value.Export(X509ContentType.Pfx)),
                HubName = name,
                Mode = PullReplicationMode.HubToSink | PullReplicationMode.SinkToHub
            }));

            // 1) Valid doc on SINK should replicate to HUB
            StoreCategory(sink, "categories/10-A", "Valid #1", "ok");

            // 2) Invalid doc on SINK should be rejected on HUB (schema), but must NOT break replication
            StoreInvalidCategoryWithExtraField(sink, "categories/11-A");

            // Ensure invalid doc did NOT replicate
            using (var session = hub.OpenSession())
            {
                var doc = session.Load<Category>("categories/11-A");
                Assert.True(doc == null, "Invalid document should not be accepted by schema on hub.");
            }

            var replicationFailureInfo = await sink.Maintenance.SendAsync(new GetReplicationOutgoingsFailureInfoOperation(nodeTag: server.ServerStore.NodeTag));
            var info = replicationFailureInfo.Stats.Single();
            Assert.True(info.Key is PullReplicationAsSink);
        }
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenEtl_ShouldValidateAndFailInNeeded()
    {
        using (var source = GetDocumentStore())
        {
            using (var destination = GetDocumentStore())
            {
                await ConfigureCategoriesSchemaAsync(destination);
                Etl.AddEtl(source, destination, "Categories", "");
                StoreInvalidCategoryWithExtraField(source, "categories/0");
                for (int i = 1; i < 5000; i++)
                {
                    StoreCategory(source, "categories/" + i, "Valid #" + i, "ok");
                }

                var count = await WaitForGreaterThanAsync(async () =>
                {
                    using var session = destination.OpenAsyncSession();
                    return await session.Query<Category>().CountAsync();
                }, 0);

                Assert.Equal(0, count);
            }
        }
    }



    private class Category
    {
        public string Name { get; set; }
        public string Description { get; set; }
    }

    private static string CategorySchemaJson = @"
{
  ""title"": ""Category"",
  ""type"": ""object"",
  ""properties"": {
    ""Name"": { ""type"": ""string"" },
    ""Description"": { ""type"": ""string"" }
  },
  ""required"": [ ""Name"" ],
  ""additionalProperties"": false
}";

    private static async Task ConfigureCategoriesSchemaAsync(IDocumentStore store)
    {
        await store.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(new SchemaValidationConfiguration
        {
            ValidatorsPerCollection = new Dictionary<string, SchemaDefinition>
            {
                ["Categories"] = new SchemaDefinition
                {
                    Schema = CategorySchemaJson
                }
            }
        }));
    }

    private static void StoreCategory(IDocumentStore store, string id, string name, string description)
    {
        using (var session = store.OpenSession())
        {
            var doc = new Category { Name = name, Description = description };
            session.Store(doc, id);
            session.SaveChanges();
        }
    }

    private static void StoreInvalidCategoryWithExtraField(IDocumentStore store, string id)
    {
        using (var session = store.OpenSession())
        {
            var invalid = new
            {
                Id = id,
                Name = "Invalid",
                Description = "Has extra field",
                ExtraField = "Not allowed"
            };

            session.Store(invalid, id);
            session.Advanced.GetMetadataFor(invalid)["@collection"] = "Categories";
            session.SaveChanges();
        }
    }


}
