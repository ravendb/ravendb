using Raven.Server.SchemaValidation;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

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

        using (var schemaDefinition = ReadObject(new DynamicJsonValue
        {
           ["patternProperties"] = new DynamicJsonValue
           {
               [Regex("[a-z]{3,}")] = new DynamicJsonValue
               {
                   ["minimum"] = 0
               },
           }
        }))
        {
            schemaValidator.Init(schemaDefinition);
        }
        
        using var validObj = ReadObject(new DynamicJsonValue
        {
            ["abc"] = 1
        });
            
        if (schemaValidator.Validate(validObj, out string errors) == false)
            Assert.Fail(string.Join("\n", errors));;
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidationPatternProperties_WhenPatternMatchAndJsonInvalid_ShouldFail()
    {
        var schemaValidator = new SchemaValidator();

        using (var schemaDefinition = ReadObject(new DynamicJsonValue
        {
           ["patternProperties"] = new DynamicJsonValue
           {
               [Regex("[a-z]{3,}")] = new DynamicJsonValue
               {
                   ["minimum"] = 0
               },
           }
        }))
        {
            schemaValidator.Init(schemaDefinition);
        }
        
        using var invalidObj = ReadObject(new DynamicJsonValue
        {
            ["abc"] = -1
        });
            
        Assert.False(schemaValidator.Validate(invalidObj, out string errors));
        AssertError("The value '-1' at 'abc' should be greater than or equal to 0.", errors);
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidationPatternProperties_WhenPatternDoesntMatch_ShouldSucceed()
    {
        var schemaValidator = new SchemaValidator();

        using (var schemaDefinition = ReadObject(new DynamicJsonValue
        {
           ["patternProperties"] = new DynamicJsonValue
           {
               [Regex("[a-z]{3,}")] = new DynamicJsonValue
               {
                   ["minimum"] = 0
               },
           }
        }))
        {
            schemaValidator.Init(schemaDefinition);
        }
        
        using var validObj = ReadObject(new DynamicJsonValue
        {
            ["ABC"] = -1
        });
            
        if (schemaValidator.Validate(validObj, out string errors) == false)
            Assert.Fail(string.Join("\n", errors));
    }
}
