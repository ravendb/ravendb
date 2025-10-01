using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.SchemaValidation;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using SVC = Raven.Server.Documents.SchemaValidation.SchemaValidatorConstants;

namespace SlowTests.SchemaValidation;

public class SchemaValidationOperationTests : ReplicationTestBase
{
    public SchemaValidationOperationTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Indexes)]
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
        
        var operation = await store.Maintenance.SendAsync(new ValidateSchemaValidationOperation(new ValidateSchemaValidationOperation.Parameters
        {
            Schema = schemaDefinition,
            Collection = "TestObjs"
        }));
        var result = await operation.WaitForCompletionAsync<ValidateSchemaValidationResult>(TimeSpan.FromMinutes(1));
        Assert.Equal(1, result.ErrorCount);
        Assert.Equal(2, result.ScannedCount);
        Assert.Equal(1, result.Errors.Count);
        Assert.True(result.Errors.TryGetValue("invalidDocId", out var error));
        Assert.Contains("The length of the value '0123456789a' at 'Prop' should not exceed 10, but its actual length is 11.", error);
    }
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task ValidateSchema_WhenLimitErrors_ShouldGetOnlyTheRequiredLimit(Options options)
    {
        const int errorDocumentCount = 10000;
        const int maxErrorsMsg = 10;
        
        using var store = GetDocumentStore(options);

        string schemaDefinition;
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            schemaDefinition =
                context.ReadObject(
                    new DynamicJsonValue { [SVC.Properties] = new DynamicJsonValue { ["Prop"] = new DynamicJsonValue { [SVC.MaxLength] = maxErrorsMsg } } },
                    "schema-validation-configuration").ToString();
        }

        await using (var session = store.BulkInsert())
        {
            for (var i = 0; i < errorDocumentCount; i++)
            {
                await session.StoreAsync(new TestObj { Prop = "0123456789a" });
            }
        }
        
        var operation = await store.Maintenance.SendAsync(new ValidateSchemaValidationOperation(new ValidateSchemaValidationOperation.Parameters
        {
            Schema = schemaDefinition,
            Collection = "TestObjs",
            MaxErrorsMsg = maxErrorsMsg,
            
        }));
        
        var result = await operation.WaitForCompletionAsync<ValidateSchemaValidationResult>(TimeSpan.FromMinutes(1));
        Assert.Equal(errorDocumentCount, result.ErrorCount);
        Assert.Equal(errorDocumentCount, result.ScannedCount);

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
    
    private class TestObj
    {
        public string Id { get; set; }
        public string Prop { get; set; }
        public object Inner { get; set; }
    }
}
