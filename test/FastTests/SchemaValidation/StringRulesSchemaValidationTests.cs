using System.Threading.Tasks;
using Raven.Server.Documents.SchemaValidation;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using SVC = Raven.Server.Documents.SchemaValidation.SchemaValidatorConstants;

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
        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Type] = "object", 
            [SVC.Properties] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue
                {
                    [SVC.MinLength] = 5
                }
            }
        };

        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);
        
        await AssertMultipleParallel(() =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                ["prop"] = "12345"
            }, out var obj);

            Assert.True(schemaValidator.Validate(obj, out var errors), errors);
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
        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Type] = "object", 
            [SVC.Properties] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue
                {
                    [SVC.MaxLength] = 5
                }
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                ["prop"] = "12345"
            }, out var obj);
            
            Assert.True(schemaValidator.Validate(obj, out var errors), errors);
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
        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Type] = "object", 
            [SVC.Properties] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue
                {
                    [SVC.Pattern] = Regex(@"[A-Za-z]{2,3}\d")
                }
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);
        
        await AssertMultipleParallel(() =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                ["prop"] = "is3"
            }, out var obj);

            Assert.True(schemaValidator.Validate(obj, out var errors), errors);
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
