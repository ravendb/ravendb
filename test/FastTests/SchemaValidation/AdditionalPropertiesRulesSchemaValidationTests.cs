using System.Threading.Tasks;
using Raven.Server.Documents.SchemaValidation;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using SVC = Raven.Server.Documents.SchemaValidation.SchemaValidatorConstants;
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
    public async Task SchemaValidation_WhenAdditionalPropertiesIsNotAllowed(bool withDefinedProp)
    {
        const string definedProp = "definedProp";
        const string notDefinedProp = "notDefinedProp";

        var schemaValidator = new SchemaValidator();
        var jsonSchemaValidator = new DynamicJsonValue
        {
            [SVC.AdditionalProperties] = false
        };
        if (withDefinedProp)
        {
            jsonSchemaValidator[SVC.Properties] = new DynamicJsonValue { [definedProp] = new DynamicJsonValue { } };
        }

        using var _ = ReadObjectOnNewCtx(jsonSchemaValidator, out var schemaDefinition);
        schemaValidator.Init(schemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                if (withDefinedProp == false)
                    return;
                
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [definedProp] = "12345" }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [notDefinedProp] = "1234" }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The property 'notDefinedProp' is not defined in the schema and additional properties are not allowed. Full path: 'notDefinedProp'.", errors);
            });
    }

    [RavenTheory(RavenTestCategory.JavaScript)]
    [InlineData(true)]
    [InlineData(false)]
    public async Task SchemaValidation_WhenHasAdditionalPropertiesRestriction(bool withDefinedProp)
    {
        const string definedProp = "definedProp";
        const string notDefinedProp = "notDefinedProp";

        var schemaValidator = new SchemaValidator();
        var jsonSchemaValidator = new DynamicJsonValue
        {
            [SVC.AdditionalProperties] = new DynamicJsonValue
            {
                [SVC.Const] = 1
            }
        };
        if (withDefinedProp)
        {
            jsonSchemaValidator[SVC.Properties] = new DynamicJsonValue { [definedProp] = new DynamicJsonValue { } };
        }

        using var _ = ReadObjectOnNewCtx(jsonSchemaValidator, out var schemaDefinition);
        schemaValidator.Init(schemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                if (withDefinedProp == false)
                    return;

                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["definedProp"] = 1 }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [notDefinedProp] = "1234" }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The value at 'notDefinedProp' must be '1', but it is '\"1234\"'.", errors);
            });
    }
}
