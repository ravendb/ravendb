using System.Diagnostics.CodeAnalysis;
using Raven.Server.SchemaValidation;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.SchemaValidation;

public class AdditionalPropertiesRulesSchemaValidationTests : SchemaValidationTestsBase
{
    // ReSharper disable once ConvertToPrimaryConstructor
    public AdditionalPropertiesRulesSchemaValidationTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.JavaScript)]
    [InlineData(true)]
    [InlineData(false)]
    public void SchemaValidation_WhenAdditionalPropertiesIsNotAllowed(bool withDefinedProp)
    {
        const string definedProp = "definedProp";
        const string notDefinedProp = "notDefinedProp";

        var schemaValidator = new SchemaValidator();
        var jsonSchemaValidator = new DynamicJsonValue
        {
            ["additionalProperties"] = false
        };
        if (withDefinedProp)
        {
            jsonSchemaValidator["properties"] = new DynamicJsonValue { [definedProp] = new DynamicJsonValue { } };
        }

        using (var schemaDefinition = ReadObject(jsonSchemaValidator))
        {
            schemaValidator.Init(schemaDefinition);
        }

        Assert.Multiple(() =>
            {
                if (withDefinedProp == false)
                    return;
                
                using var validObj = ReadObject(new DynamicJsonValue { ["definedProp"] = "12345" });

                if (schemaValidator.Validate(validObj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var invalidObj = ReadObject(new DynamicJsonValue { [notDefinedProp] = "1234" });

                Assert.False(schemaValidator.Validate(invalidObj, out var errors));
                AssertError("The property 'notDefinedProp' at '' is not defined and additional properties are not allowed.", errors);
            });
    }

    [RavenTheory(RavenTestCategory.JavaScript)]
    [InlineData(true)]
    [InlineData(false)]
    public void SchemaValidation_WhenHasAdditionalPropertiesRestriction(bool withDefinedProp)
    {
        const string definedProp = "definedProp";
        const string notDefinedProp = "notDefinedProp";

        var schemaValidator = new SchemaValidator();
        var jsonSchemaValidator = new DynamicJsonValue
        {
            ["additionalProperties"] = new DynamicJsonValue
            {
                ["const"] = 1
            }
        };
        if (withDefinedProp)
        {
            jsonSchemaValidator["properties"] = new DynamicJsonValue { [definedProp] = new DynamicJsonValue { } };
        }

        using (var schemaDefinition = ReadObject(jsonSchemaValidator))
        {
            schemaValidator.Init(schemaDefinition);
        }

        Assert.Multiple(() =>
            {
                if(withDefinedProp == false)
                    return;
                
                using var validObj = ReadObject(new DynamicJsonValue { ["definedProp"] = 1 });

                if (schemaValidator.Validate(validObj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var invalidObj = ReadObject(new DynamicJsonValue { [notDefinedProp] = "1234" });

                Assert.False(schemaValidator.Validate(invalidObj, out var errors));
                AssertError("The value at 'notDefinedProp' must be '1', but it is '1234'.", errors);
            });
    }
}
