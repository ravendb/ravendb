using Raven.Server.SchemaValidation;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.SchemaValidation;

public class GeneralSchemaValidationTests : SchemaValidationTestsBase
{
    public GeneralSchemaValidationTests(ITestOutputHelper output) : base(output)
    {

    }

    [RavenTheory(RavenTestCategory.JavaScript)]
    [InlineData(true)]
    [InlineData(false)]
    public void SchemaValidation_WhenPropIsRequired(bool withAdditionalRestriction)
    {
        const string prop = "prop";

        var schemaValidator = new SchemaValidator();
        var dynamicJsonValue = new DynamicJsonValue { ["type"] = "object", ["required"] = new DynamicJsonArray { prop } };
        if (withAdditionalRestriction)
        {
            dynamicJsonValue["properties"] = new DynamicJsonValue { [prop] = new DynamicJsonValue { ["type"] = "string" } };
        }

        using (var schemaDefinition = ReadObject(dynamicJsonValue))
        {
            schemaValidator.Init(schemaDefinition);
        }

        Assert.Multiple(() =>
            {
                var validObj = ReadObject(new DynamicJsonValue { [prop] = "123" });

                if (schemaValidator.Validate(validObj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                var invalidObj = ReadObject(new DynamicJsonValue { ["prop1"] = "123" });

                Assert.False(schemaValidator.Validate(invalidObj, out var errors));
                AssertError("The required property 'prop' is missing at 'prop'.", errors);
            });
    }
}
