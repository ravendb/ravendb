using System.Diagnostics.CodeAnalysis;
using Raven.Server.SchemaValidation;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.SchemaValidation;

public class StringRulesSchemaValidationTests : SchemaValidationTestsBase
{
    // ReSharper disable once ConvertToPrimaryConstructor
    public StringRulesSchemaValidationTests(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateMinStringLength()
    {
        var schemaValidator = new SchemaValidator();
        using (var schemaDefinition = ReadObject(new DynamicJsonValue
        {
           ["type"] = "object", 
           ["properties"] = new DynamicJsonValue
           {
               ["prop"] = new DynamicJsonValue
               {
                   ["minLength"] = 5
               }
           }
        }))
        {
            schemaValidator.Init(schemaDefinition);
        }
        
        Assert.Multiple(() =>
        {
            using var validObj = ReadObject(new DynamicJsonValue
            {
                ["prop"] = "12345"
            });

            if (schemaValidator.Validate(validObj, out string errors) == false)
                Assert.Fail(string.Join("\n", errors));
        },
        () =>
        {
            using var invalidObj = ReadObject(new DynamicJsonValue
            {
                ["prop"] = "1234"
            });

            Assert.False(schemaValidator.Validate(invalidObj, out var errors));
            AssertError("The length of the value at 'root.prop' should be at least 5, but its actual length is 4.", errors);
        });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateMaxStringLength()
    {
        var schemaValidator = new SchemaValidator();
        using (var schemaDefinition = ReadObject(new DynamicJsonValue
        {
           ["type"] = "object", 
           ["properties"] = new DynamicJsonValue
           {
               ["prop"] = new DynamicJsonValue
               {
                   ["maxLength"] = 5
               }
           }
        }))
        {
            schemaValidator.Init(schemaDefinition);
        }

        Assert.Multiple(() =>
        {
            using var validObj = ReadObject(new DynamicJsonValue
            {
                ["prop"] = "12345"
            });
            
            if (schemaValidator.Validate(validObj, out string errors) == false)
                Assert.Fail(string.Join("\n", errors));
        },
        () =>
        {
            using var invalidObj = ReadObject(new DynamicJsonValue
            {
                ["prop"] = "123456"
            });
            Assert.False(schemaValidator.Validate(invalidObj, out var errors));
            AssertError("The length of the value at 'root.prop' should not exceed 5, but its actual length is 6.", errors);
        });
    }

    private string Regex([StringSyntax(StringSyntaxAttribute.Regex)] string pattern) => pattern;
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateForRegexPattern()
    {
        var schemaValidator = new SchemaValidator();
        using (var schemaDefinition = ReadObject(new DynamicJsonValue
        {
           ["type"] = "object", 
           ["properties"] = new DynamicJsonValue
           {
               ["prop"] = new DynamicJsonValue
               {
                   ["pattern"] = Regex(@"[A-Za-z]{2,3}\d")
               }
           }
        }))
        {
            schemaValidator.Init(schemaDefinition);
        }
        
        Assert.Multiple(() =>
        {
            var validObj = ReadObject(new DynamicJsonValue
            {
                ["prop"] = "is3"
            });

            if (schemaValidator.Validate(validObj, out string errors) == false)
                Assert.Fail(string.Join("\n", errors));
        },
        () =>
        {
            var invalidObj = ReadObject(new DynamicJsonValue
            {
                ["prop"] = "i3"
            });

            Assert.False(schemaValidator.Validate(invalidObj, out var errors));
            AssertError("The value 'i3' at 'root.prop' does not match the required pattern '[A-Za-z]{2,3}\\d'.", errors);
        });
    }
}
