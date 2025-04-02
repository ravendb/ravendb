using System.Threading.Tasks;
using Raven.Server.SchemaValidation;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using SVC = Raven.Server.SchemaValidation.SchemaValidatorConstants;

namespace FastTests.SchemaValidation;

public class PropertyNamesSchemaValidationTests : SchemaValidationTestsBase
{
    // ReSharper disable once ConvertToPrimaryConstructor
    public PropertyNamesSchemaValidationTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenRestrictPropertyNamesPattern()
    {
        var schemaValidator = new SchemaValidator(ContextPool);
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.PropertyNames] = new DynamicJsonValue
            {
                [SVC.Pattern] = "^[A-Z]+$"
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
                    ["ABC"] = "somevalue"
                }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["abc"] = "somevalue",
                }, out var obj);
                
                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The pattern of the property name 'abc' at '' does not match the required pattern '^[A-Z]+$'.", errors);
            });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenRestrictPropertyNamesMinLength()
    {
        var schemaValidator = new SchemaValidator(ContextPool);
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.PropertyNames] = new DynamicJsonValue
            {
                [SVC.MinLength] = 3
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
                    ["123"] = "somevalue"
                }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["1234"] = "somevalue"
                }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["12"] = "somevalue",
                }, out var obj);
                
                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The length of the property name '12' at '' should be at least 3, but its actual length is 2.", errors);
            });
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenRestrictPropertyNamesMaxLength()
    {
        var schemaValidator = new SchemaValidator(ContextPool);
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.PropertyNames] = new DynamicJsonValue
            {
                [SVC.MaxLength] = 5
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
                    ["123"] = "somevalue"
                }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["1234"] = "somevalue"
                }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["123456"] = "somevalue",
                }, out var obj);
                
                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The length of the property name '123456' at '' should not exceed 5, but its actual length is 6.", errors);
            });
    }
}
