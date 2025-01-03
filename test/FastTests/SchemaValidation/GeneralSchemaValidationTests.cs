using System.Threading.Tasks;
using Raven.Server.SchemaValidation;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using SVC = Raven.Server.SchemaValidation.SchemaValidatorConstants;

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
        var schemaDefinition = new DynamicJsonValue { [SVC.@type] = "object", ["required"] = new DynamicJsonArray { prop } };
        if (withAdditionalRestriction)
        {
            schemaDefinition[SVC.properties] = new DynamicJsonValue { [prop] = new DynamicJsonValue { [SVC.@type] = "string" } };
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
            [SVC.properties] = new DynamicJsonValue
            {
                [prop] = new DynamicJsonValue
                {
                    [SVC.properties] = new DynamicJsonValue
                    {
                        [prop] = new DynamicJsonValue
                        {
                            [SVC.@const] = 123
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

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenRestrictOnMinProperties()
    {
        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.minProperties] = 2
        };
        using (ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition))
        {
            schemaValidator.Init(blitSchemaDefinition);
        }

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["prop1"] = "value1",
                    ["prop2"] = "value2",
                }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["prop1"] = "value1",
                    ["prop2"] = "value2",
                    ["prop3"] = "value3",
                }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["prop1"] = "value1",
                }, out var obj);
                
                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The object at '' must have at least 2 properties, but it has only 1.", errors);
            });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenRestrictOnMaxProperties()
    {
        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.maxProperties] = 3
        };
        using (ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition))
        {
            schemaValidator.Init(blitSchemaDefinition);
        }

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["prop1"] = "value1",
                    ["prop2"] = "value2",
                    ["prop2"] = "value2",
                }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["prop1"] = "value1",
                    ["prop2"] = "value2",
                }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["prop1"] = "value1",
                    ["prop2"] = "value2",
                    ["prop3"] = "value3",
                    ["prop4"] = "value4",
                }, out var obj);
                
                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The object at '' must have no more than 3 properties, but it has 4.", errors);
            });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenRestrictOnUniqueItems()
    {
        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.properties] = new DynamicJsonValue
            {
                ["prop1"] = new DynamicJsonValue
                {
                    [SVC.uniqueItems] = true
                }
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
                    ["prop1"] = new DynamicJsonArray{1, 2, 3},
                }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["prop1"] = 1,
                }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["prop1"] = new DynamicJsonArray{1, 2, 1},
                }, out var obj);
                
                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The array at 'prop1' contains duplicate value: '1'. Each item must be unique.", errors);
            });
    }
}
