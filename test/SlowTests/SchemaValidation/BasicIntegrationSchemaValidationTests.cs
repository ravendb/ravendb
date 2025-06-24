using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Operations.SchemaValidation;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents.BulkInsert;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using SVC = Raven.Server.SchemaValidation.SchemaValidatorConstants;

namespace SlowTests.SchemaValidation;

public class BasicIntegrationSchemaValidationTests : ReplicationTestBase
{
    public BasicIntegrationSchemaValidationTests(ITestOutputHelper output) : base(output)
    {
    }

    private class TestObj
    {
        public string Id { get; set; }
        public string Prop { get; set; }
        public string Prop2 { get; set; }
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenStoreDocument_ShouldValidateAndFailInNeeded()
    {
        var schemaDefinitionObj = new DynamicJsonValue { [SVC.Properties] = new DynamicJsonValue { ["Prop"] = new DynamicJsonValue { [SVC.Const] = "123" } } };

        using var context = JsonOperationContext.ShortTermSingleUse();
        using var schemaDefinition = context.ReadObject(schemaDefinitionObj, "test object");
        using var store = GetDocumentStore();
        var configuration = new SchemaValidationConfiguration
        {
            Disabled = false,
            ValidatorsByCollection = new Dictionary<string, SchemaValidationConfiguration.Validator>
            {
                { "TestObjs", new SchemaValidationConfiguration.Validator { SchemaDefinition = schemaDefinition.ToString() } }
            }
        };
        await store.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(configuration));

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj { Prop = "123" });
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj { Prop = "1234" });
            var e = await Assert.ThrowsAnyAsync<RavenException>(async () => await session.SaveChangesAsync());
            Assert.StartsWith("Raven.Client.Exceptions.SchemaValidationException: The value at 'Prop' must be '\"123\"', but it is '\"1234\"'.", e.Message);
        }
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenSingleDocumentPatch_ShouldValidateAndFailInNeeded()
    {
        const string id = "random-id";

        var schemaDefinitionObj = new DynamicJsonValue { [SVC.Properties] = new DynamicJsonValue { ["Prop"] = new DynamicJsonValue { [SVC.MaxLength] = 3 } } };

        using var context = JsonOperationContext.ShortTermSingleUse();
        using var schemaDefinition = context.ReadObject(schemaDefinitionObj, "test object");
        using var store = GetDocumentStore();
        var configuration = new SchemaValidationConfiguration
        {
            Disabled = false,
            ValidatorsByCollection = new Dictionary<string, SchemaValidationConfiguration.Validator>
            {
                { "TestObjs", new SchemaValidationConfiguration.Validator { SchemaDefinition = schemaDefinition.ToString() } }
            }
        };
        await store.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(configuration));

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj(), id);
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            var loaded = await session.LoadAsync<TestObj>(id);
            session.Advanced.Patch(loaded, x => x.Prop, "123");
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            var loaded = await session.LoadAsync<TestObj>(id);
            session.Advanced.Patch(loaded, x => x.Prop, "1234");
            var e = await Assert.ThrowsAnyAsync<RavenException>(async () => await session.SaveChangesAsync());
            Assert.StartsWith("Raven.Client.Exceptions.SchemaValidationException: The length of the value '1234' at 'Prop' should not exceed 3, but its actual length is 4.", e.Message);
        }
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenSetBasedPatch_ShouldValidateAndFailInNeeded()
    {
        const string id = "random-id";

        var schemaDefinitionObj = new DynamicJsonValue { [SVC.Properties] = new DynamicJsonValue { ["Prop"] = new DynamicJsonValue { [SVC.MaxLength] = 3 } } };

        using var context = JsonOperationContext.ShortTermSingleUse();
        using var schemaDefinition = context.ReadObject(schemaDefinitionObj, "test object");
        using var store = GetDocumentStore();
        var configuration = new SchemaValidationConfiguration
        {
            Disabled = false,
            ValidatorsByCollection = new Dictionary<string, SchemaValidationConfiguration.Validator>
            {
                { "TestObjs", new SchemaValidationConfiguration.Validator { SchemaDefinition = schemaDefinition.ToString() } }
            }
        };
        await store.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(configuration));

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj(), id);
            await session.SaveChangesAsync();
        }

        var operation = await store.Operations.SendAsync(new PatchByQueryOperation("from TestObjs update {this.Prop = '123'}"));
        await operation.WaitForCompletionAsync();

        var e = await Assert.ThrowsAnyAsync<RavenException>(async () =>
        {
            var op = await store.Operations.SendAsync(new PatchByQueryOperation("from TestObjs update {this.Prop = '1234'}"));
            await op.WaitForCompletionAsync();
        });
        Assert.StartsWith("Raven.Client.Exceptions.SchemaValidationException: The length of the value '1234' at 'Prop' should not exceed 3, but its actual length is 4.", e.Message);
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenBulkInsert_ShouldValidateAndFailInNeeded()
    {
        const string id = "random-id";

        var schemaDefinitionObj = new DynamicJsonValue { [SVC.Properties] = new DynamicJsonValue { ["Prop"] = new DynamicJsonValue { [SVC.MaxLength] = 3 } } };

        using var context = JsonOperationContext.ShortTermSingleUse();
        using var schemaDefinition = context.ReadObject(schemaDefinitionObj, "test object");
        using var store = GetDocumentStore();
        var configuration = new SchemaValidationConfiguration
        {
            Disabled = false,
            ValidatorsByCollection = new Dictionary<string, SchemaValidationConfiguration.Validator>
            {
                { "TestObjs", new SchemaValidationConfiguration.Validator { SchemaDefinition = schemaDefinition.ToString() } }
            }
        };
        await store.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(configuration));

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj(), id);
            await session.SaveChangesAsync();
        }

        var operation = await store.Operations.SendAsync(new PatchByQueryOperation("from TestObjs update {this.Prop = '123'}"));
        await operation.WaitForCompletionAsync();

        var e = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using var bulkInsert = store.BulkInsert();
            await bulkInsert.StoreAsync(new TestObj { Prop = "1234" }, id);
        });

        while (e.InnerException != null)
            e = e.InnerException;
        
        Assert.IsType<BulkInsertAbortedException>(e);
        var message = e.Message.Split(Environment.NewLine)[1].TrimStart();
        Assert.StartsWith("---> Raven.Client.Exceptions.SchemaValidationException: The length of the value '1234' at 'Prop' should not exceed 3, but its actual length is 4.", message);
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenImportInvalidDataDefinedOnTheSource_ShouldNotThrow()
    {
        var schemaDefinitionObj = new DynamicJsonValue { [SVC.Properties] = new DynamicJsonValue { ["Prop"] = new DynamicJsonValue { [SVC.Const] = "123" } } };

        using var context = JsonOperationContext.ShortTermSingleUse();
        using var schemaDefinition = context.ReadObject(schemaDefinitionObj, "test object");
        using var source = GetDocumentStore();

        const string id = "random-id";
        using (var session = source.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj { Prop = "1234" }, id);
            await session.SaveChangesAsync();
        }

        using var destination = GetDocumentStore();
        var configuration = new SchemaValidationConfiguration
        {
            Disabled = false,
            ValidatorsByCollection = new Dictionary<string, SchemaValidationConfiguration.Validator>
            {
                { "TestObjs", new SchemaValidationConfiguration.Validator { SchemaDefinition = schemaDefinition.ToString() } }
            }
        };
        await source.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(configuration));

        var e = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            var operation = await source.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), destination.Smuggler);
            await operation.WaitForCompletionAsync();
        });
        Assert.StartsWith("Raven.Client.Exceptions.SchemaValidationException: The value at 'Prop' must be '\"123\"', but it is '\"1234\"'.", e.Message);
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenImportInvalidDataDefinedOnTheDestination_ShouldNotThrow()
    {
        var schemaDefinitionObj = new DynamicJsonValue { [SVC.Properties] = new DynamicJsonValue { ["Prop"] = new DynamicJsonValue { [SVC.Const] = "123" } } };

        using var context = JsonOperationContext.ShortTermSingleUse();
        using var schemaDefinition = context.ReadObject(schemaDefinitionObj, "test object");
        using var source = GetDocumentStore();

        const string id = "random-id";
        using (var session = source.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj { Prop = "1234" }, id);
            await session.SaveChangesAsync();
        }

        using var destination = GetDocumentStore();
        var configuration = new SchemaValidationConfiguration
        {
            Disabled = false,
            ValidatorsByCollection = new Dictionary<string, SchemaValidationConfiguration.Validator>
            {
                { "TestObjs", new SchemaValidationConfiguration.Validator { SchemaDefinition = schemaDefinition.ToString() } }
            }
        };
        await destination.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(configuration));

        var e = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            var operation = await source.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), destination.Smuggler);
            await operation.WaitForCompletionAsync();
        });
        Assert.StartsWith("Raven.Client.Exceptions.SchemaValidationException: The value at 'Prop' must be '\"123\"', but it is '\"1234\"'.", e.Message);
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenRestoreInvalidData_ShouldNotThrow()
    {
        var schemaDefinitionObj = new DynamicJsonValue { [SVC.Properties] = new DynamicJsonValue { ["Prop"] = new DynamicJsonValue { [SVC.Const] = "123" } } };

        using var context = JsonOperationContext.ShortTermSingleUse();
        using var schemaDefinition = context.ReadObject(schemaDefinitionObj, "test object");
        using var source = GetDocumentStore();

        const string id = "random-id";
        using (var session = source.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj { Prop = "1234" }, id);
            await session.SaveChangesAsync();
        }

        var configuration = new SchemaValidationConfiguration
        {
            Disabled = false,
            ValidatorsByCollection = new Dictionary<string, SchemaValidationConfiguration.Validator>
            {
                { "TestObjs", new SchemaValidationConfiguration.Validator { SchemaDefinition = schemaDefinition.ToString() } }
            }
        };
        await source.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(configuration));

        //TODO To change to shard as well
        var backupPath = NewDataPath();
        await Backup.CreateAndRunBackupAsync(source, RavenDatabaseMode.Single, backupPath);

        var destinationName = GetDatabaseName();
        using var _ = Backup.RestoreDatabase(source,
            new RestoreBackupConfiguration { DatabaseName = destinationName, BackupLocation = Directory.GetDirectories(backupPath).First(), });
        using var destination = new DocumentStore { Urls = source.Urls, Database = destinationName, }.Initialize();
        
        var e = await Assert.ThrowsAnyAsync<RavenException>(async () =>
        {
            using var session = destination.OpenAsyncSession();
            var loaded = await session.LoadAsync<TestObj>(id);
            loaded.Prop2 = "something";
            await session.SaveChangesAsync();
        });
        Assert.StartsWith("Raven.Client.Exceptions.SchemaValidationException: The value at 'Prop' must be '\"123\"', but it is '\"1234\"'.", e.Message);
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenEtl_ShouldValidateAndFailInNeeded()
    {
        var schemaDefinitionObj = new DynamicJsonValue { [SVC.Properties] = new DynamicJsonValue { ["Prop"] = new DynamicJsonValue { [SVC.Const] = "123" } } };

        using var context = JsonOperationContext.ShortTermSingleUse();
        using var schemaDefinition = context.ReadObject(schemaDefinitionObj, "test object");
        using var source = GetDocumentStore();

        const string id = "random-id";
        using (var session = source.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj { Prop = "1234" }, id);
            await session.SaveChangesAsync();
        }

        using var destination = GetDocumentStore();
        var configuration = new SchemaValidationConfiguration
        {
            Disabled = false,
            ValidatorsByCollection = new Dictionary<string, SchemaValidationConfiguration.Validator>
            {
                { "TestObjs", new SchemaValidationConfiguration.Validator { SchemaDefinition = schemaDefinition.ToString() } }
            }
        };
        await destination.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(configuration));

        Etl.AddEtl(source, destination, "TestObjs", script: "loadToTestObjs(this)");

        var client = source.GetRequestExecutor().HttpClient;
        await AssertWaitForTrueAsync(async () =>
        {
            var response = await client.GetAsync($"{source.Urls.First()}/databases/{source.Database}/etl/stats");
            var json = await context.ReadForMemoryAsync(await response.Content.ReadAsStreamAsync(), "etl/test/script");
            return HasError(json);
        });
        return;

        bool HasError(BlittableJsonReaderObject json)
        {
            if (json.TryGet("Results", out BlittableJsonReaderArray results) == false || results.Length == 0 || results.First() is not BlittableJsonReaderObject result)
                return false;

            if (result.TryGet(nameof(EtlTaskStats.Stats), out BlittableJsonReaderArray stats) == false || stats.Length == 0 ||
                stats.First() is not BlittableJsonReaderObject stat)
                return false;

            if (stat.TryGet(nameof(EtlProcessTransformationStats.Statistics), out BlittableJsonReaderObject statistics) == false)
                return false;

            if (statistics.TryGet(nameof(EtlProcessStatistics.LastAlert), out BlittableJsonReaderObject lastAlert) == false || lastAlert == null)
                return false;

            if (lastAlert.TryGet(nameof(AlertRaised.Details), out BlittableJsonReaderObject details) == false)
                return false;

            if (details.TryGet(nameof(EtlErrorsDetails.Errors), out BlittableJsonReaderArray errors) == false || errors.Length == 0 ||
                errors.First() is not BlittableJsonReaderObject errorInfo)
                return false;

            if(errorInfo.TryGet(nameof(EtlErrorInfo.Error), out string error) == false || string.IsNullOrEmpty(error))
                return false;
            
            return error.StartsWith("Raven.Client.Exceptions.SchemaValidationException: Raven.Client.Exceptions.SchemaValidationException: The value at 'Prop' must be '\"123\"', but it is '\"1234\"");
        }
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenImportToDestinationWithSchemaValidation_ShouldValidateAndFailInNeeded()
    {
        var schemaDefinitionObj = new DynamicJsonValue { [SVC.Properties] = new DynamicJsonValue { ["Prop"] = new DynamicJsonValue { [SVC.Const] = "123" } } };

        using var context = JsonOperationContext.ShortTermSingleUse();
        using var schemaDefinition = context.ReadObject(schemaDefinitionObj, "test object");
        using var source = GetDocumentStore();

        const string id = "random-id";
        using (var session = source.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj { Prop = "1234" }, id);
            await session.SaveChangesAsync();
        }

        using var destination = GetDocumentStore();
        var configuration = new SchemaValidationConfiguration
        {
            Disabled = false,
            ValidatorsByCollection = new Dictionary<string, SchemaValidationConfiguration.Validator>
            {
                { "TestObjs", new SchemaValidationConfiguration.Validator { SchemaDefinition = schemaDefinition.ToString() } }
            }
        };
        await destination.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(configuration));

        var e = await Assert.ThrowsAnyAsync<RavenException>(async () =>
        {
            var operation = await source.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), destination.Smuggler);
            await operation.WaitForCompletionAsync();
        });
        Assert.StartsWith("Raven.Client.Exceptions.SchemaValidationException: The value at 'Prop' must be '\"123\"', but it is '\"1234\"'.", e.Message);

        using (var session = source.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj { Prop = "1234" }, id);
            await session.SaveChangesAsync();
        }

        await source.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), destination.Smuggler);
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenInternalReplicateInvalidData_ShouldNotThrow()
    {
        var (nodes, _) = await CreateRaftCluster(3);
        
        var schemaDefinitionObj = new DynamicJsonValue { [SVC.Properties] = new DynamicJsonValue { ["Prop"] = new DynamicJsonValue { [SVC.Const] = "123" } } };

        using var context = JsonOperationContext.ShortTermSingleUse();
        using var schemaDefinition = context.ReadObject(schemaDefinitionObj, "test object");
        
        using var store = GetDocumentStore(new Options
        {
            Server = nodes[0],
            ReplicationFactor = 1,
            ModifyDatabaseRecord = x => x.Topology = new DatabaseTopology { Members = [nodes[0].ServerStore.NodeTag] }
        });

        const string id = "random-id";
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj { Prop = "1234" }, id);
            await session.SaveChangesAsync();
        }

        var configuration = new SchemaValidationConfiguration
        {
            Disabled = false,
            ValidatorsByCollection = new Dictionary<string, SchemaValidationConfiguration.Validator>
            {
                { "TestObjs", new SchemaValidationConfiguration.Validator { SchemaDefinition = schemaDefinition.ToString() } }
            }
        };
        await store.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(configuration));

        await store.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(store.Database, nodes[1].ServerStore.NodeTag));
        
        using var mentee = new DocumentStore
        {
            Urls = [nodes[1].WebUrl], 
            Database = store.Database, 
            Conventions = { DisableTopologyUpdates = true }
        }.Initialize();

        await AssertWaitForTrueAsync(async () =>
        {
            using var session = mentee.OpenAsyncSession();
            var load = await session.LoadAsync<TestObj>(id);
            return load != null;
        });
        
        var e = await Assert.ThrowsAnyAsync<RavenException>(async () =>
        {
            using var session = mentee.OpenAsyncSession();
            var load = await session.LoadAsync<TestObj>(id);
            load.Prop2 = "something";
            await session.SaveChangesAsync();
        });
        Assert.StartsWith("Raven.Client.Exceptions.SchemaValidationException: The value at 'Prop' must be '\"123\"', but it is '\"1234\"'.", e.Message);
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenExternalReplicateInvalidData_ShouldNotThrow()
    {
        var schemaDefinitionObj = new DynamicJsonValue { [SVC.Properties] = new DynamicJsonValue { ["Prop"] = new DynamicJsonValue { [SVC.Const] = "123" } } };

        using var context = JsonOperationContext.ShortTermSingleUse();
        using var schemaDefinition = context.ReadObject(schemaDefinitionObj, "test object");
        using var source = GetDocumentStore();

        const string id = "random-id";
        using (var session = source.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj { Prop = "1234" }, id);
            await session.SaveChangesAsync();
        }

        using var destination = GetDocumentStore();
        var configuration = new SchemaValidationConfiguration
        {
            Disabled = false,
            ValidatorsByCollection = new Dictionary<string, SchemaValidationConfiguration.Validator>
            {
                { "TestObjs", new SchemaValidationConfiguration.Validator { SchemaDefinition = schemaDefinition.ToString() } }
            }
        };
        await destination.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(configuration));

        await SetupReplicationAsync(source, destination);

        await AssertWaitForTrueAsync(async () =>
        {
            using var session = destination.OpenAsyncSession();
            var load = await session.LoadAsync<TestObj>(id);
            return load != null;
        });
        
        var e = await Assert.ThrowsAnyAsync<RavenException>(async () =>
        {
            using var session = destination.OpenAsyncSession();
            var load = await session.LoadAsync<TestObj>(id);
            load.Prop2 = "something";
            await session.SaveChangesAsync();
        });
        Assert.StartsWith("Raven.Client.Exceptions.SchemaValidationException: The value at 'Prop' must be '\"123\"', but it is '\"1234\"'.", e.Message);
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenRevertRevisionToInvalidData_ShouldNotThrow()
    {
        var schemaDefinitionObj = new DynamicJsonValue { [SVC.Properties] = new DynamicJsonValue { ["Prop"] = new DynamicJsonValue { [SVC.Const] = "123" } } };

        using var context = JsonOperationContext.ShortTermSingleUse();
        using var schemaDefinition = context.ReadObject(schemaDefinitionObj, "test object");
        using var store = GetDocumentStore();

        await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore);
        
        const string id = "random-id";
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj { Prop = "1234" }, id);
            await session.SaveChangesAsync();
        }

        var configuration = new SchemaValidationConfiguration
        {
            Disabled = false,
            ValidatorsByCollection = new Dictionary<string, SchemaValidationConfiguration.Validator>
            {
                { "TestObjs", new SchemaValidationConfiguration.Validator { SchemaDefinition = schemaDefinition.ToString() } }
            }
        };
        await store.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(configuration));

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj { Prop = "123" }, id);
            await session.SaveChangesAsync();
        }
        
        using (var session = store.OpenAsyncSession())
        {
            var revisions = await session.Advanced.Revisions
                .GetMetadataForAsync(id);
            
            var changeVector = revisions[1].GetString(Constants.Documents.Metadata.ChangeVector);
            
            var e = await Assert.ThrowsAnyAsync<RavenException>(async () => await store.Operations.SendAsync(new RevertRevisionsByIdOperation(id, changeVector)));
            Assert.StartsWith("Raven.Client.Exceptions.SchemaValidationException: The value at 'Prop' must be '\"123\"', but it is '\"1234\"'.", e.Message);
        }
    }
}
