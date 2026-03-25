using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.SchemaValidation;
using Raven.Server.Config;
using Raven.Server.Documents.SchemaValidation;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.SchemaValidation;

public class SchemaValidationConfigurationTests : ReplicationTestBase
{
    public SchemaValidationConfigurationTests(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenExceedMaxDepth_ShouldThrow()
    {
        var settings = new Dictionary<string, string>
        {
            [RavenConfiguration.GetKey(x => x.SchemaValidation.MaxDepth)] = "2"
        };
        var server = GetNewServer(new ServerCreationOptions{CustomSettings = settings});
        
        var schemaDefinitionObj = new DynamicJsonValue
        {
            [SchemaValidatorConstants.Properties] = new DynamicJsonValue
            {
                ["Nested"] = new DynamicJsonValue
                {
                    [SchemaValidatorConstants.Properties] = new DynamicJsonValue
                    {
                        ["Nested"] = new DynamicJsonValue
                        {
                            [SchemaValidatorConstants.Properties] = new DynamicJsonValue
                            {
                                ["Prop"] = new DynamicJsonValue
                                {
                                    [SchemaValidatorConstants.Const] = "123"
                                }
                            }
                        }
                    }
                }
            }
        };

        using var context = JsonOperationContext.ShortTermSingleUse();
        using var schemaDefinition = context.ReadObject(schemaDefinitionObj, "test object");
        using var store = GetDocumentStore(new Options{Server = server});
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
            await session.StoreAsync(new TestObj { Nested = new TestObj { Nested = new TestObj { Prop = "123" }} });
            var e = await Assert.ThrowsAnyAsync<Exception>(async () => await session.SaveChangesAsync());
            Assert.Contains("Raven.Client.Exceptions.SchemaValidation.SchemaValidationException: Maximum validation path depth of 2 exceeded.", e.Message);
        }
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenExceedRegexTimeout_ShouldThrow()
    {
        const string propValue = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!";
        const string pattern = "^([a-z]+)+$";
        
        var settings = new Dictionary<string, string>
        {
            [RavenConfiguration.GetKey(x => x.SchemaValidation.RegexTimeout)] = "1"
        };
        var server = GetNewServer(new ServerCreationOptions{CustomSettings = settings});
        
        var schemaDefinitionObj = new DynamicJsonValue
        {
            [SchemaValidatorConstants.Properties] = new DynamicJsonValue
            {
                ["Prop"] = new DynamicJsonValue
                {
                    [SchemaValidatorConstants.Pattern] = pattern
                }
            }
        };

        using var context = JsonOperationContext.ShortTermSingleUse();
        using var schemaDefinition = context.ReadObject(schemaDefinitionObj, "test object");
        using var store = GetDocumentStore(new Options{Server = server});
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
            await session.StoreAsync(new TestObj { Prop = propValue });
            var e = await Assert.ThrowsAnyAsync<Exception>(async () => await session.SaveChangesAsync());
            Assert.Contains("Raven.Client.Exceptions.SchemaValidation.SchemaValidationException: The pattern matching of the value 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!' at 'Prop' timed out for pattern '^([a-z]+)+$'.", e.Message);
        }
    }
}

public class TestObj
{
    public string Prop { get; set; }
    public TestObj Nested { get; set; }
    public string Prop2 { get; set; }
}
