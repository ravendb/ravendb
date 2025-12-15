using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.DataArchival;
using Raven.Client.Documents.Operations.Refresh;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.SchemaValidation;
using Raven.Client.Exceptions.SchemaValidation;
using Raven.Client.Util;
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


    [RavenFact(RavenTestCategory.Core)]
    public async Task RefreshShouldSkipSchemaValiadtion()
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

    [RavenFact(RavenTestCategory.Core)]
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
}
