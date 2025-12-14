using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.SchemaValidation;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.SchemaValidation;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using SVC = Raven.Server.Documents.SchemaValidation.SchemaValidatorConstants;

namespace SlowTests.SchemaValidation;

public class SchemaValidationExternalReplicationTests : ReplicationTestBase
{
    public SchemaValidationExternalReplicationTests(ITestOutputHelper output) : base(output)
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
                ValidatorsPerCollection = new Dictionary<string, SchemaDefinition>()
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

    private class TestObj
    {
        public string Id { get; set; }
        public string Prop { get; set; }
    }
}
