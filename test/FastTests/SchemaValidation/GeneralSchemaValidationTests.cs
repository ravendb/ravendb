using System.Threading.Tasks;
using Raven.Server.Documents.SchemaValidation;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using SVC = Raven.Server.Documents.SchemaValidation.SchemaValidatorConstants;

namespace FastTests.SchemaValidation;

public class GeneralSchemaValidationTests : SchemaValidationTestsBase
{
    // ReSharper disable once ConvertToPrimaryConstructor
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
        var schemaDefinition = new DynamicJsonValue { [SVC.Type] = "object", [SVC.Required] = new DynamicJsonArray { prop } };
        if (withAdditionalRestriction)
        {
            schemaDefinition[SVC.Properties] = new DynamicJsonValue { [prop] = new DynamicJsonValue { [SVC.Type] = "string" } };
        }

        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [prop] = "123" }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
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
            [SVC.Properties] = new DynamicJsonValue
            {
                [prop] = new DynamicJsonValue { [SVC.Properties] = new DynamicJsonValue { [prop] = new DynamicJsonValue { [SVC.Const] = 123 } } }
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [prop] = new DynamicJsonValue { [prop] = 123 } }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [prop] = new DynamicJsonValue { [prop] = 1234 } }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The value at 'prop.prop' must be '123', but it is '1234'.", errors);
            });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenRestrictOnMinProperties()
    {
        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue { [SVC.MinProperties] = 2 };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop1"] = "value1", ["prop2"] = "value2", }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop1"] = "value1", ["prop2"] = "value2", ["prop3"] = "value3", }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop1"] = "value1", }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The object at '' must have at least 2 properties, but it has only 1.", errors);
            });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenRestrictOnMaxProperties()
    {
        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue { [SVC.MaxProperties] = 3 };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop1"] = "value1", ["prop2"] = "value2", ["prop3"] = "value3", }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop1"] = "value1", ["prop2"] = "value2", }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["prop1"] = "value1", ["prop2"] = "value2", ["prop3"] = "value3", ["prop4"] = "value4",
                }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The object at '' must have no more than 3 properties, but it has 4.", errors);
            });
    }
}
