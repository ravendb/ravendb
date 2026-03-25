using Raven.Server.Documents.SchemaValidation;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using SVC = Raven.Server.Documents.SchemaValidation.SchemaValidatorConstants;

namespace FastTests.SchemaValidation;

public class PatternPropertiesSchemaValidationTests : SchemaValidationTestsBase
{
    public PatternPropertiesSchemaValidationTests(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidationPatternProperties_WhenPatternMatchAndJsonValid_ShouldSucceed()
    {
        var schemaValidator = new SchemaValidator();

        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.PatternProperties] = new DynamicJsonValue
            {
                [Regex("[a-z]{3,}")] = new DynamicJsonValue
                {
                    [SVC.Minimum] = 0
                },
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);
        
        using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
        {
            ["abc"] = 1
        }, out var obj);
            
        Assert.True(schemaValidator.Validate(obj, out var errors), errors);
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidationPatternProperties_WhenPatternMatchAndJsonInvalid_ShouldFail()
    {
        var schemaValidator = new SchemaValidator();

        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.PatternProperties] = new DynamicJsonValue
            {
                [Regex("[a-z]{3,}")] = new DynamicJsonValue
                {
                    [SVC.Minimum] = 0
                },
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);
        
        using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
        {
            ["abc"] = -1
        }, out var obj);
            
        Assert.False(schemaValidator.Validate(obj, out string errors));
        AssertError("The value '-1' at 'abc' should be greater than or equal to 0.", errors);
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidationPatternProperties_WhenPatternDoesntMatch_ShouldSucceed()
    {
        var schemaValidator = new SchemaValidator();

        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.PatternProperties] = new DynamicJsonValue
            {
                [Regex("[a-z]{3,}")] = new DynamicJsonValue
                {
                    [SVC.Minimum] = 0
                },
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);
        
        using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
        {
            ["ABC"] = -1
        }, out var obj);
            
        Assert.True(schemaValidator.Validate(obj, out var errors), errors);
    }
}
