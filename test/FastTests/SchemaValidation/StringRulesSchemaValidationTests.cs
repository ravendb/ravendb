using System.Threading.Tasks;
using Raven.Server.SchemaValidation;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using SVC = Raven.Server.SchemaValidation.SchemaValidatorConstants;

namespace FastTests.SchemaValidation;

public class StringRulesSchemaValidationTests : SchemaValidationTestsBase
{
    // ReSharper disable once ConvertToPrimaryConstructor
    public StringRulesSchemaValidationTests(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenValidateMinStringLength()
    {
        var schemaValidator = new SchemaValidator(ContextPool);
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.@type] = "object", 
            [SVC.properties] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue
                {
                    [SVC.minLength] = 5
                }
            }
        };
        using (ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition))
        {
            schemaValidator.Init(blitSchemaDefinition);
        }
        
        await AssertMultipleParallel(() =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                ["prop"] = "12345"
            }, out var obj);

            if (schemaValidator.Validate(obj, out string errors) == false)
                Assert.Fail(string.Join("\n", errors));
        },
        () =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                ["prop"] = "1234"
            }, out var obj);

            Assert.False(schemaValidator.Validate(obj, out var errors));
            AssertError("The length of the value '1234' at 'prop' should be at least 5, but its actual length is 4.", errors);
        });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenValidateMaxStringLength()
    {
        var schemaValidator = new SchemaValidator(ContextPool);
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.@type] = "object", 
            [SVC.properties] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue
                {
                    [SVC.maxLength] = 5
                }
            }
        };
        using (ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition))
        {
            schemaValidator.Init(blitSchemaDefinition);
        }

        await AssertMultipleParallel(() =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                ["prop"] = "12345"
            }, out var obj);
            
            if (schemaValidator.Validate(obj, out string errors) == false)
                Assert.Fail(string.Join("\n", errors));
        },
        () =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                ["prop"] = "123456"
            }, out var obj);
            Assert.False(schemaValidator.Validate(obj, out var errors));
            AssertError("The length of the value '123456' at 'prop' should not exceed 5, but its actual length is 6.", errors);
        });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenValidateForRegexPattern()
    {
        var schemaValidator = new SchemaValidator(ContextPool);
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.@type] = "object", 
            [SVC.properties] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue
                {
                    [SVC.pattern] = Regex(@"[A-Za-z]{2,3}\d")
                }
            }
        };
        using (ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition))
        {
            schemaValidator.Init(blitSchemaDefinition);
        }
        
        await AssertMultipleParallel(() =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                ["prop"] = "is3"
            }, out var obj);

            if (schemaValidator.Validate(obj, out string errors) == false)
                Assert.Fail(string.Join("\n", errors));
        },
        () =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                ["prop"] = "i3"
            }, out var obj);

            Assert.False(schemaValidator.Validate(obj, out var errors));
            AssertError("The pattern of the value 'i3' at 'prop' does not match the required pattern '[A-Za-z]{2,3}\\d'.", errors);
        });
    }
}
