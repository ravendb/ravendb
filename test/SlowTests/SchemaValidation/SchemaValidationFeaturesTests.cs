using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Refresh;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.SchemaValidation;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.SchemaValidation;
using Raven.Tests.Core.Utils.Entities;
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

    [RavenTheory(RavenTestCategory.Indexes)]
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
                s1.Advanced.WaitForReplicationAfterSaveChanges(TimeSpan.FromSeconds(5));
                
                await Assert.ThrowsAsync<RavenTimeoutException>(async () => await s1.SaveChangesAsync());
            }

            var replicationFailureInfo = await store1.Maintenance.SendAsync(new GetReplicationOutgoingsFailureInfoOperation(nodeTag: "A", shardNumber: 0));

            Assert.NotNull(replicationFailureInfo.Stats);
            Assert.Equal(1, replicationFailureInfo.Stats.Count);

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

    private class TestObj
    {
        public string Id { get; set; }
        public string Prop { get; set; }
    }
}
