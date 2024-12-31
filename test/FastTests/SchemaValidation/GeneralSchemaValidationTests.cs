using System.Threading.Tasks;
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
    public async Task SchemaValidation_WhenPropIsRequired(bool withAdditionalRestriction)
    {
        const string prop = "prop";

        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue { ["type"] = "object", ["required"] = new DynamicJsonArray { prop } };
        if (withAdditionalRestriction)
        {
            schemaDefinition["properties"] = new DynamicJsonValue { [prop] = new DynamicJsonValue { ["type"] = "string" } };
        }

        using (ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition))
        {
            schemaValidator.Init(blitSchemaDefinition);
        }

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [prop] = "123" }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop1"] = "123" }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The required property 'prop' is missing at ''.", errors);
            });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenHasRestrictionOnNestedObject()
    {
        const string prop = "prop";

        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            ["properties"] = new DynamicJsonValue
            {
                [prop] = new DynamicJsonValue
                {
                    ["properties"] = new DynamicJsonValue
                    {
                        [prop] = new DynamicJsonValue
                        {
                            ["const"] = 123
                        }
                    }
                }
            }
        };
        using (ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition))
        {
            schemaValidator.Init(blitSchemaDefinition);
        }

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [prop] = new DynamicJsonValue { [prop] = 123 } }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [prop] = new DynamicJsonValue { [prop] = 1234 } }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The value at 'prop.prop' must be '123', but it is '1234'.", errors);
            });
    }
}
