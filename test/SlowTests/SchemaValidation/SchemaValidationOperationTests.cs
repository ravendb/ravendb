using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.SchemaValidation;
using Raven.Client.Exceptions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using SVC = Raven.Server.Documents.SchemaValidation.SchemaValidatorConstants;

namespace SlowTests.SchemaValidation;

public class SchemaValidationOperationTests : RavenTestBase
{
    public SchemaValidationOperationTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.JavaScript)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task ValidateSchema_WhenDone_ShouldAbleToFetchResult(Options options)
    {
        const string invalidDocId = "invalidDocId";
        const string validDocId = "validDocId";
        
        using var store = GetDocumentStore(options);

        string schemaDefinition;
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            schemaDefinition =
                context.ReadObject(
                    new DynamicJsonValue { [SVC.Properties] = new DynamicJsonValue { ["Prop"] = new DynamicJsonValue { [SVC.MaxLength] = 10 } } },
                    "schema-validation-configuration").ToString();
        }

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj { Prop = "0123456789a" }, invalidDocId);
            await session.StoreAsync(new TestObj { Prop = "01" }, validDocId);
            await session.SaveChangesAsync();
        }
        
        var operation = await store.Maintenance.SendAsync(new StartSchemaValidationOperation(new StartSchemaValidationOperation.Parameters
        {
            SchemaDefinition = schemaDefinition,
            Collection = "TestObjs"
        }));
        var result = await operation.WaitForCompletionAsync<ValidateSchemaResult>(TimeSpan.FromMinutes(1));
        Assert.Equal(1, result.ErrorCount);
        Assert.Equal(2, result.ValidatedCount);
        Assert.Equal(1, result.Errors.Count);
        Assert.True(result.Errors.TryGetValue("invalidDocId", out var error));
        Assert.Contains("The length of the value '0123456789a' at 'Prop' should not exceed 10, but its actual length is 11.", error);
    }
    
    [RavenTheory(RavenTestCategory.JavaScript)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task ValidateSchema_WhenLimitErrors_ShouldGetOnlyTheRequiredLimit(Options options)
    {
        const int errorDocumentCount = 10000;
        const int maxErrorsMsg = 10;
        const int maxLength = 10;
        
        using var store = GetDocumentStore(options);

        string schemaDefinition;
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            schemaDefinition =
                context.ReadObject(
                    new DynamicJsonValue { [SVC.Properties] = new DynamicJsonValue { ["Prop"] = new DynamicJsonValue { [SVC.MaxLength] = maxLength } } },
                    "schema-validation-configuration").ToString();
        }

        await using (var session = store.BulkInsert())
        {
            for (var i = 0; i < errorDocumentCount; i++)
            {
                await session.StoreAsync(new TestObj { Prop = "0123456789a" });
            }
        }
        
        var operation = await store.Maintenance.SendAsync(new StartSchemaValidationOperation(new StartSchemaValidationOperation.Parameters
        {
            SchemaDefinition = schemaDefinition,
            Collection = "TestObjs",
            MaxErrorMessages = maxErrorsMsg
            
        }));
        
        var result = await operation.WaitForCompletionAsync<ValidateSchemaResult>(TimeSpan.FromMinutes(1));
        Assert.Equal(errorDocumentCount, result.ErrorCount);
        Assert.Equal(errorDocumentCount, result.ValidatedCount);

        var expectedErrors = maxErrorsMsg;
        if (options.DatabaseMode == RavenDatabaseMode.Sharded)
        {
            var configuration = await Sharding.GetShardingConfigurationAsync(store);
            var shardsCount = configuration.Shards.Count;
            expectedErrors = maxErrorsMsg / shardsCount * shardsCount; //If the requested doesn't divide equally between the shards we will get fewer errors
        }
        Assert.Equal(expectedErrors, result.Errors.Count);
        foreach (var keyValue in result.Errors)
        {
            Assert.Contains("The length of the value '0123456789a' at 'Prop' should not exceed 10, but its actual length is 11.", keyValue.Value);
        }
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task ValidateSchemaOperation_WhenSettingEtagOnNonSharded_ShouldStartFromTheEtag()
    {
        const string id1 = "user/1";
        const string id2 = "user/2";
        const int maxLength = 10;
        
        using var store = GetDocumentStore();

        string schemaDefinition;
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            schemaDefinition =
                context.ReadObject(
                    new DynamicJsonValue { [SVC.Properties] = new DynamicJsonValue { ["Prop"] = new DynamicJsonValue { [SVC.MaxLength] = maxLength } } },
                    "schema-validation-configuration").ToString();
        }

        await using (var session = store.BulkInsert())
        {
            await session.StoreAsync(new TestObj { Prop = "0123456789a" }, id1);
            await session.StoreAsync(new TestObj { Prop = "0123456789ab"}, id2);
        }

        var operation1 = await store.Maintenance.SendAsync(new StartSchemaValidationOperation(new StartSchemaValidationOperation.Parameters
        {
            SchemaDefinition = schemaDefinition,
            Collection = "TestObjs",
            MaxDocumentsToValidate = 1
        }));

        var result1 = await operation1.WaitForCompletionAsync<ValidateSchemaResult>(TimeSpan.FromMinutes(1));
        Assert.Equal(id1, result1.Errors.First().Key);
        Assert.StartsWith("The length of the value '0123456789a' at 'Prop' should not exceed 10, but its actual length is 11.", result1.Errors.First().Value);

        var operation2 = await store.Maintenance.SendAsync(new StartSchemaValidationOperation(new StartSchemaValidationOperation.Parameters
        {
            SchemaDefinition = schemaDefinition,
            Collection = "TestObjs",
            StartEtag = result1.LastEtag + 1

        }));

        var result2 = await operation2.WaitForCompletionAsync<ValidateSchemaResult>(TimeSpan.FromMinutes(1));
        Assert.Equal(id2, result2.Errors.First().Key);
        Assert.StartsWith("The length of the value '0123456789ab' at 'Prop' should not exceed 10, but its actual length is 12.", result2.Errors.First().Value);
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task ValidateSchemaOperation_WhenSettingEtagOnSharded_ShouldFail()
    {
        const int maxLength = 10;
        
        using var store = GetDocumentStore(Options.ForMode(RavenDatabaseMode.Sharded));

        string schemaDefinition;
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            schemaDefinition =
                context.ReadObject(
                    new DynamicJsonValue { [SVC.Properties] = new DynamicJsonValue { ["Prop"] = new DynamicJsonValue { [SVC.MaxLength] = maxLength } } },
                    "schema-validation-configuration").ToString();
        }
        
        var e = await Assert.ThrowsAnyAsync<BadRequestException>(async () =>
        {
            await store.Maintenance.SendAsync(new StartSchemaValidationOperation(new StartSchemaValidationOperation.Parameters
            {
                SchemaDefinition = schemaDefinition,
                Collection = "TestObjs",
                StartEtag = 1
            }));
        });
        Assert.Contains("Parameter 'StartEtag' is not supported for schema validation on a sharded database", e.Message);
    }
    
    private class TestObj
    {
        public string Id { get; set; }
        public string Prop { get; set; }
        public object Inner { get; set; }
    }
}
