using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Operations.SchemaValidation;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.SchemaValidation;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using SVC = Raven.Server.Documents.SchemaValidation.SchemaValidatorConstants;

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

    public static IEnumerable<object[]> AdditionalPropertiesTestCases
    {
        get
        {
            var additionalPropertiesDefinition = new (object, string)[]
            {
                (false, "The property 'Prop2' is not defined in the schema and additional properties are not allowed. Full path: 'Prop2'"),
                (new DynamicJsonValue { [SVC.Type] = SchemaValidationHelper.Integer }, "'Prop2' should be of type 'integer' but actual type is 'string'.")
            };
            foreach (var additionalProperties in additionalPropertiesDefinition)
            {
                foreach (var propertiesDefined in new object[]{true, false})
                {
                    yield return [additionalProperties.Item1, propertiesDefined, additionalProperties.Item2];
                }
            }
        }
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenDefineCollectionTwice_ShouldThrow()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();
        Assert.ThrowsAny<ArgumentException>(() => _ = new SchemaValidationConfiguration()
        {
            Disabled = false,
            ValidatorsPerCollection = new Dictionary<string, SchemaDefinition>
            {
                { "TestObjs", new SchemaDefinition() },
                { "testobjs", new SchemaDefinition() },
            }
        });

        using var store = GetDocumentStore();
        var requestExecutor = store.GetRequestExecutor();
        var client = requestExecutor.HttpClient;
        
        var data = new StringContent("{\"ValidatorsPerCollection\": {\"TestObjs\": {}, \"testobjs\": {}}}", Encoding.UTF8, "application/json");
        var response = await client.PostAsync($"{store.Urls.First()}/databases/{store.Database}/admin/schema-validation/config", data);
        var r = await response.Content.ReadAsStringAsync();
        Assert.Contains("An item with the same key has already been added", r);
    }

    [RavenTheory(RavenTestCategory.JavaScript)]
    [MemberData(nameof(AdditionalPropertiesTestCases))]
    public async Task SchemaValidation_WhenAdditionalPropertiesAreRestricted_ShouldAcceptMetadata(object additionalProperties, bool propertiesDefined, string error)
    {
        var schemaDefinitionObj = new DynamicJsonValue
        {
            [SVC.AdditionalProperties] = additionalProperties,
        };

        if (propertiesDefined)
            schemaDefinitionObj[SVC.Properties] = new DynamicJsonValue { ["Prop"] = new DynamicJsonValue { [SVC.Const] = "123" } };
        
        using var context = JsonOperationContext.ShortTermSingleUse();
        using var schemaDefinition = context.ReadObject(schemaDefinitionObj, "test object");
        using var store = GetDocumentStore(new Options
        {
            ModifyDocumentStore = s =>
            {
                s.OnAfterConversionToDocument += (sender, args) =>
                {
                    var properties = args.Entity.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance);

                    foreach (var property in properties)
                    {
                        if (property.Name != "Id" && property.GetValue(args.Entity) == null)
                            (args.Document.Modifications ??= new DynamicJsonValue(args.Document)).Remove(property.Name);
                    }
                    if(args.Document.Modifications != null)
                        args.Document = args.Session.Context.ReadObject(args.Document, args.Id);
                };
            }
        });
        var configuration = new SchemaValidationConfiguration
        {
            Disabled = false,
            ValidatorsPerCollection = new Dictionary<string, SchemaDefinition>
            {
                { "TestObjs", new SchemaDefinition { Schema = schemaDefinition.ToString() } }
            }
        };
        await store.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(configuration));

        using (var session = store.OpenAsyncSession())
        {
            var testObj = new TestObj();
            if(propertiesDefined)
                testObj.Prop = "123";
            await session.StoreAsync(testObj);
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj{ Prop2 = "something" });
            var e = await Assert.ThrowsAnyAsync<RavenException>(async () => await session.SaveChangesAsync());
            AssertError(error, e.Message);
        }
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
            ValidatorsPerCollection = new Dictionary<string, SchemaDefinition>
            {
                { "TestObjs", new SchemaDefinition { Schema = schemaDefinition.ToString() } }
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
            Assert.StartsWith("Raven.Client.Exceptions.SchemaValidation.SchemaValidationException: The value at 'Prop' must be '\"123\"', but it is '\"1234\"'.", e.Message);
        }
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenDefineOnEmptyCollection_ShouldValidateAndFailInNeeded()
    {
        var schemaDefinitionObj = new DynamicJsonValue { [SVC.Properties] = new DynamicJsonValue { ["Prop"] = new DynamicJsonValue { [SVC.Const] = "123" } } };

        using var context = JsonOperationContext.ShortTermSingleUse();
        using var schemaDefinition = context.ReadObject(schemaDefinitionObj, "test object");
        using var store = GetDocumentStore();
        var configuration = new SchemaValidationConfiguration
        {
            Disabled = false,
            ValidatorsPerCollection = new Dictionary<string, SchemaDefinition>
            {
                { Constants.Documents.Collections.EmptyCollection, new SchemaDefinition { Schema = schemaDefinition.ToString() } }
            }
        };
        await store.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(configuration));

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new { Prop = "1234" });
            var e = await Assert.ThrowsAnyAsync<RavenException>(async () => await session.SaveChangesAsync());
            Assert.StartsWith("Raven.Client.Exceptions.SchemaValidation.SchemaValidationException: The value at 'Prop' must be '\"123\"', but it is '\"1234\"'.", e.Message);
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
            ValidatorsPerCollection = new Dictionary<string, SchemaDefinition>
            {
                { "TestObjs", new SchemaDefinition { Schema = schemaDefinition.ToString() } }
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
            Assert.StartsWith("Raven.Client.Exceptions.SchemaValidation.SchemaValidationException: The length of the value '1234' at 'Prop' should not exceed 3, but its actual length is 4.", e.Message);
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
            ValidatorsPerCollection = new Dictionary<string, SchemaDefinition>
            {
                { "TestObjs", new SchemaDefinition { Schema = schemaDefinition.ToString() } }
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
        Assert.StartsWith("Raven.Client.Exceptions.SchemaValidation.SchemaValidationException: The length of the value '1234' at 'Prop' should not exceed 3, but its actual length is 4.", e.Message);
    }
    
    [RavenFact(RavenTestCategory.Smuggler)]
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
            ValidatorsPerCollection = new Dictionary<string, SchemaDefinition>
            {
                { "TestObjs", new SchemaDefinition { Schema = schemaDefinition.ToString() } }
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

        var stringException = e.ToString();
        Assert.True(stringException.Contains("The length of the value '1234' at 'Prop' should not exceed 3, but its actual length is 4."), $"actual: {stringException}");
    }

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Smuggler)]
    [RavenData(true, DatabaseMode = RavenDatabaseMode.All)]
    [RavenData(false, DatabaseMode = RavenDatabaseMode.All)]
    public async Task SchemaValidation_WhenImportInvalidDataDocuments_ShouldSkip(Options options, bool configOnDestination)
    {
        var schemaDefinitionObj = new DynamicJsonValue { [SVC.Properties] = new DynamicJsonValue { ["Prop"] = new DynamicJsonValue { [SVC.Const] = "123" } } };

        using var context = JsonOperationContext.ShortTermSingleUse();
        using var schemaDefinition = context.ReadObject(schemaDefinitionObj, "test object");
        
        var modifyRecord = options.ModifyDatabaseRecord;
        var option = new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.ConflictSolverConfig = new ConflictSolver
                {
                    ResolveToLatest = false,
                    ResolveByCollection = new Dictionary<string, ScriptResolver>()
                };
                modifyRecord?.Invoke(record);
            }
        };
        using var sourceA = GetDocumentStore(option);
        using var sourceB = GetDocumentStore(option);

        await RevisionsHelper.SetupRevisionsAsync(sourceA);
        
        const string idConflict = "conflict-doc-id";
        const string idRevisions = "revisions-id";
        const string id = "random-id";
        using (var session = sourceA.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj { Prop = "1234" }, idConflict);
            await session.StoreAsync(new TestObj { Prop = "1234" }, id);
            var testObj = new TestObj { Prop = "1234" };
            await session.StoreAsync(testObj, idRevisions);
            await session.SaveChangesAsync();

            testObj.Prop = "123";
            await session.SaveChangesAsync();
        }
        
        using (var session = sourceB.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj { Prop = "2234" }, idConflict);
            await session.SaveChangesAsync();
        }
        
        await SetupReplicationAsync(sourceB, sourceA);
        WaitUntilHasConflict(sourceA, idConflict);

        using var destination = GetDocumentStore(option);
        var configuration = new SchemaValidationConfiguration
        {
            Disabled = false,
            ValidatorsPerCollection = new Dictionary<string, SchemaDefinition>
            {
                { "TestObjs", new SchemaDefinition { Schema = schemaDefinition.ToString() } }
            }
        };
        
        await (configOnDestination ? destination : sourceA).Maintenance.SendAsync(new ConfigureSchemaValidationOperation(configuration));

        var operation = await sourceA.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), destination.Smuggler);
        await operation.WaitForCompletionAsync();

        
        using (var sourceSession = destination.OpenAsyncSession())
        using (var destinationSession = destination.OpenAsyncSession())
        {
            Assert.Null(await destinationSession.LoadAsync<TestObj>(id));
            Assert.NotNull(await destinationSession.LoadAsync<TestObj>(idRevisions));

            var sourceRevisions = await GetRevisions(sourceSession);
            var destinationRevisions = await GetRevisions(destinationSession);
            Assert.Subset(sourceRevisions.ToHashSet(), destinationRevisions.ToHashSet());
            
            var conflicts = await destination.Commands().GetConflictsForAsync(idConflict);
            Assert.Equal(2, conflicts.Length);
            
            async Task<IEnumerable<string>> GetRevisions(IAsyncDocumentSession asyncDocumentSession)
                => (await asyncDocumentSession.Advanced.Revisions.GetForAsync<TestObj>(idRevisions)).Select(x => asyncDocumentSession.Advanced.GetChangeVectorFor(x));
        }
    }

    [RavenFact(RavenTestCategory.Smuggler | RavenTestCategory.BackupExportImport)]
    public async Task SchemaValidation_WhenRestoreInvalidData_ShouldRestore()
    {
        var schemaDefinitionObj = new DynamicJsonValue { [SVC.Properties] = new DynamicJsonValue { ["Prop"] = new DynamicJsonValue { [SVC.Const] = "123" } } };

        using var context = JsonOperationContext.ShortTermSingleUse();
        using var schemaDefinition = context.ReadObject(schemaDefinitionObj, "test object");
        using var source = GetDocumentStore();

        const string id = "random-id";
        const string invalidValue = "1234";
        using (var session = source.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj { Prop = invalidValue }, id);
            await session.SaveChangesAsync();
        }

        var configuration = new SchemaValidationConfiguration
        {
            Disabled = false,
            ValidatorsPerCollection = new Dictionary<string, SchemaDefinition>
            {
                { "TestObjs", new SchemaDefinition { Schema = schemaDefinition.ToString() } }
            }
        };
        await source.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(configuration));

        var backupPath = NewDataPath();
        await Backup.CreateAndRunBackupAsync(source, RavenDatabaseMode.Single, backupPath);

        var destinationName = GetDatabaseName();
        using var _ = Backup.RestoreDatabase(source,
            new RestoreBackupConfiguration { DatabaseName = destinationName, BackupLocation = Directory.GetDirectories(backupPath).First(), });
        using var destination = new DocumentStore { Urls = source.Urls, Database = destinationName, }.Initialize();

        using (var session = destination.OpenAsyncSession())
        {
            var loaded = await session.LoadAsync<TestObj>(id);
            Assert.Equal(invalidValue, loaded.Prop);
        }
        
        var e = await Assert.ThrowsAnyAsync<RavenException>(async () =>
        {
            using var session = destination.OpenAsyncSession();
            var loaded = await session.LoadAsync<TestObj>(id);
            loaded.Prop2 = "something";
            await session.SaveChangesAsync();
        });
        Assert.StartsWith("Raven.Client.Exceptions.SchemaValidation.SchemaValidationException: The value at 'Prop' must be '\"123\"', but it is '\"1234\"'.", e.Message);
    }

    [RavenFact(RavenTestCategory.JavaScript | RavenTestCategory.Etl)]
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
            ValidatorsPerCollection = new Dictionary<string, SchemaDefinition>
            {
                { "TestObjs", new SchemaDefinition { Schema = schemaDefinition.ToString() } }
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
            
            return error.StartsWith("Raven.Client.Exceptions.SchemaValidation.SchemaValidationException: Raven.Client.Exceptions.SchemaValidation.SchemaValidationException: The value at 'Prop' must be '\"123\"', but it is '\"1234\"");
        }
    }

    [RavenFact(RavenTestCategory.JavaScript | RavenTestCategory.Replication)]
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
            ValidatorsPerCollection = new Dictionary<string, SchemaDefinition>
            {
                { "TestObjs", new SchemaDefinition { Schema = schemaDefinition.ToString() } }
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
        Assert.StartsWith("Raven.Client.Exceptions.SchemaValidation.SchemaValidationException: The value at 'Prop' must be '\"123\"', but it is '\"1234\"'.", e.Message);
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenDefineSchemaOnMetadata_ShouldReject()
    {
        var schemaDefinitionObj = new DynamicJsonValue { [SVC.Properties] = new DynamicJsonValue { [Constants.Documents.Metadata.Key] = new DynamicJsonValue { [SVC.Const] = "123" } } };

        using var context = JsonOperationContext.ShortTermSingleUse();
        using var schemaDefinition = context.ReadObject(schemaDefinitionObj, "test object");
        using var store = GetDocumentStore();

        var configuration = new SchemaValidationConfiguration
        {
            Disabled = false,
            ValidatorsPerCollection = new Dictionary<string, SchemaDefinition>
            {
                { "TestObjs", new SchemaDefinition { Schema = schemaDefinition.ToString() } }
            }
        };
        var e = await Assert.ThrowsAnyAsync<RavenException>(async () => await store.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(configuration)));
        Assert.Contains("Define a schema validation on metadata is not allowed.", e.Message);
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenDefineInvalidSchema_ShouldReject()
    {
        var schemaDefinitionObj = new DynamicJsonValue { [SVC.Properties] = "ShouldBeObject" };

        using var context = JsonOperationContext.ShortTermSingleUse();
        using var schemaDefinition = context.ReadObject(schemaDefinitionObj, "test object");
        using var store = GetDocumentStore();

        var configuration = new SchemaValidationConfiguration
        {
            Disabled = false,
            ValidatorsPerCollection = new Dictionary<string, SchemaDefinition>
            {
                { "TestObjs", new SchemaDefinition { Schema = schemaDefinition.ToString() } }
            }
        };
        var e = await Assert.ThrowsAnyAsync<RavenException>(async () => await store.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(configuration)));
        Assert.Contains("The value of 'properties' must be an object, but received 'ShouldBeObject' of type 'string'. Schema path '#/properties'.", e.Message);
    }
    
    [RavenFact(RavenTestCategory.JavaScript | RavenTestCategory.Revisions)]
    public async Task SchemaValidation_WhenRevertRevisionAndSchemaIsEnabled_ShouldThrowWhen()
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
            ValidatorsPerCollection = new Dictionary<string, SchemaDefinition>
            {
                { "TestObjs", new SchemaDefinition { Schema = schemaDefinition.ToString() } }
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
            Assert.Contains("Reverting documents to revisions is not allowed when Schema Validation is enabled. Please disable Schema Validation and try again.", e.Message);
            
            configuration.Disabled = true;
            await store.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(configuration));

            await store.Operations.SendAsync(new RevertRevisionsByIdOperation(id, changeVector));
        }
    }

    [RavenFact(RavenTestCategory.JavaScript | RavenTestCategory.Revisions)]
    public async Task SchemaValidation_WhenRevertRevisionByTimeSchemaIsEnabled_ShouldThrow()
    {
        var schemaDefinitionObj = new DynamicJsonValue { [SVC.Properties] = new DynamicJsonValue { ["Prop"] = new DynamicJsonValue { [SVC.MaxLength] = 3 } } };

        using var context = JsonOperationContext.ShortTermSingleUse();
        using var schemaDefinition = context.ReadObject(schemaDefinitionObj, "test object");
        using var store = GetDocumentStore();

        await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore);
        
        DateTime revisionTime;
        
        const string withInvalidRevisions = "withInvalidRevisions";
        const string withOnlyValidRevisions = "withOnlyValidRevisions";
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj { Prop = "1234" }, withInvalidRevisions);
            await session.StoreAsync(new TestObj { Prop = "123" }, withOnlyValidRevisions);
            await session.SaveChangesAsync();
            revisionTime = DateTime.UtcNow;
        }

        var configuration = new SchemaValidationConfiguration
        {
            Disabled = false,
            ValidatorsPerCollection = new Dictionary<string, SchemaDefinition>
            {
                { "TestObjs", new SchemaDefinition { Schema = schemaDefinition.ToString() } }
            }
        };
        await store.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(configuration));

        await Task.Delay(TimeSpan.FromSeconds(10));
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj { Prop = "12" }, withInvalidRevisions);
            await session.StoreAsync(new TestObj { Prop = "12" }, withOnlyValidRevisions);
            await session.SaveChangesAsync();
        }

        var e = await Assert.ThrowsAnyAsync<RavenException>(async () =>
        {
            var operation = await store.Maintenance.SendAsync(new RevisionsHelper.RevertRevisionsOperation(revisionTime, 1));
            await operation.WaitForCompletionAsync<RevertResult>(TimeSpan.FromSeconds(5));
        });
        Assert.Contains("Reverting documents to revisions is not allowed when Schema Validation is enabled. Please disable Schema Validation and try again.", e.Message);
        
        configuration.Disabled = true;
        await store.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(configuration));
        
        var operation = await store.Maintenance.SendAsync(new RevisionsHelper.RevertRevisionsOperation(revisionTime, 1));
        await operation.WaitForCompletionAsync<RevertResult>(TimeSpan.FromSeconds(5));
    }
    
    private static void AssertError(string expected, string actual)
    {
        if (actual.Contains(expected) == false)
            Assert.Fail($"expected: {expected}, actual: {actual}");
    }
}
