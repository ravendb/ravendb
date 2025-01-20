using System.Threading.Tasks;
using Raven.Server.SchemaValidation;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.SchemaValidation;

public class ConditionalSchemaValidationTests : SchemaValidationTestsBase
{
    // ReSharper disable once ConvertToPrimaryConstructor
    public ConditionalSchemaValidationTests(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenValidateConditionalSchema()
    {
        const string ifProp = "ifProp";
        const string thenProp = "thenProp";
        const string elseProp = "elseProp";

        var schemaValidator = new SchemaValidator(ContextPool);
        var schemaDefinition = new DynamicJsonValue
        {
            [SchemaValidatorConstants.@if] = new DynamicJsonValue
            {
                [SchemaValidatorConstants.required] = new []{ifProp}
            },
            [SchemaValidatorConstants.then] = new DynamicJsonValue
            {
                [SchemaValidatorConstants.required] = new []{thenProp}
            },
            [SchemaValidatorConstants.@else] = new DynamicJsonValue
            {
                [SchemaValidatorConstants.required] = new []{elseProp}
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
                    [ifProp] = "123",
                    [thenProp] = "123"
                }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    [elseProp] = "123"
                }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["anotherProp"] = "123"
                }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The required property 'elseProp' is missing at ''.", errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    [ifProp] = "123",
                }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The required property 'thenProp' is missing at ''.", errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The required property 'elseProp' is missing at ''.", errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["anotherProp"] = "123",
                }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The required property 'elseProp' is missing at ''.", errors);
            });
    }
}
