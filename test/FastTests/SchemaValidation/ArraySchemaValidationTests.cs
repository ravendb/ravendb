using System.Threading.Tasks;
using Raven.Server.SchemaValidation;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.SchemaValidation;

public class ArraySchemaValidationTests : SchemaValidationTestsBase
{
    // ReSharper disable once ConvertToPrimaryConstructor
    public  ArraySchemaValidationTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenRestrictOnUniqueItems()
    {
        var schemaValidator = new SchemaValidator(ContextPool);
        var schemaDefinition = new DynamicJsonValue { [SchemaValidatorConstants.properties] = new DynamicJsonValue { ["prop1"] = new DynamicJsonValue { [SchemaValidatorConstants.uniqueItems] = true } } };
        using (ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition))
        {
            schemaValidator.Init(blitSchemaDefinition);
        }

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop1"] = new DynamicJsonArray { 1, 2, 3 }, }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop1"] = 1, }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop1"] = new DynamicJsonArray { 1, 2, 1 }, }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The array at 'prop1' contains duplicate value: '1'. Each item must be unique.", errors);
            });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenRestrictOnArrayPrefixItems()
    {
        const string prop = "somepropname";

        var schemaValidator = new SchemaValidator(ContextPool);
        var schemaDefinition = new DynamicJsonValue
        {
            [SchemaValidatorConstants.properties] = new DynamicJsonValue
            {
                [prop] = new DynamicJsonValue
                {
                    [SchemaValidatorConstants.prefixItems] = new DynamicJsonArray
                    {
                        new DynamicJsonValue { [SchemaValidatorConstants.type] = "integer" }, new DynamicJsonValue { [SchemaValidatorConstants.type] = "string" },
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
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [prop] = new DynamicJsonArray { 1 }, }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [prop] = new DynamicJsonArray { 1, "somestring" }, }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [prop] = new DynamicJsonArray { 1 }, }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [prop] = new DynamicJsonArray { 1, "somestring", "additional" }, }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [prop] = new DynamicJsonArray { "" } }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("'somepropname[0]' should be of type 'integer' but actual type is 'string'.", errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [prop] = new DynamicJsonArray { 1, 2 } }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("'somepropname[1]' should be of type 'string' but actual type is 'integer'.", errors);
            });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenRestrictOnArrayItemsFalse_ShouldNotAllowAdditionalItems()
    {
        const string prop = "somepropname";

        var schemaValidator = new SchemaValidator(ContextPool);
        var schemaDefinition = new DynamicJsonValue
        {
            [SchemaValidatorConstants.properties] = new DynamicJsonValue
            {
                [prop] = new DynamicJsonValue
                {
                    [SchemaValidatorConstants.prefixItems] = new DynamicJsonArray
                    {
                        new DynamicJsonValue { [SchemaValidatorConstants.type] = "integer" }, new DynamicJsonValue { [SchemaValidatorConstants.type] = "string" },
                    },
                    [SchemaValidatorConstants.items] = false
                }
            }
        };
        using (ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition))
        {
            schemaValidator.Init(blitSchemaDefinition);
        }

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [prop] = new DynamicJsonArray { 1, "" }, }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [prop] = new DynamicJsonArray { 1, "", 2 } }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The array at 'somepropname' contains additional items, which are not allowed.", errors);
            });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenRestrictOnArrayItems()
    {
        const string prop = "somepropname";

        var schemaValidator = new SchemaValidator(ContextPool);
        var schemaDefinition = new DynamicJsonValue
        {
            [SchemaValidatorConstants.properties] = new DynamicJsonValue { [prop] = new DynamicJsonValue { [SchemaValidatorConstants.items] = new DynamicJsonValue { [SchemaValidatorConstants.type] = "string" } } }
        };
        using (ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition))
        {
            schemaValidator.Init(blitSchemaDefinition);
        }

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [prop] = new DynamicJsonArray { "somestring", "somestring2" }, }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [prop] = new DynamicJsonArray { 1, "", 2 } }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError(@"'somepropname[0]' should be of type 'string' but actual type is 'integer'.
'somepropname[2]' should be of type 'string' but actual type is 'integer'", errors);
            });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenRestrictOnArrayContains()
    {
        const string prop = "somepropname";

        var schemaValidator = new SchemaValidator(ContextPool);
        var schemaDefinition = new DynamicJsonValue
        {
            [SchemaValidatorConstants.properties] = new DynamicJsonValue { [prop] = new DynamicJsonValue { [SchemaValidatorConstants.contains] = new DynamicJsonValue { [SchemaValidatorConstants.type] = "string" } } }
        };
        using (ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition))
        {
            schemaValidator.Init(blitSchemaDefinition);
        }

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [prop] = new DynamicJsonArray { 1, "somestring2" }, }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [prop] = new DynamicJsonArray { 1, 2 } }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError(
                    "The array at 'somepropname' must contain at least 1 items matching the required schema, but no items where found. Schema : {\"type\":\"string\"}",
                    errors);
            });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenRestrictOnArrayContainsWithMaxRestriction()
    {
        const string prop = "somepropname";

        var schemaValidator = new SchemaValidator(ContextPool);
        var schemaDefinition = new DynamicJsonValue
        {
            [SchemaValidatorConstants.properties] = new DynamicJsonValue
            {
                [prop] = new DynamicJsonValue { [SchemaValidatorConstants.contains] = new DynamicJsonValue { [SchemaValidatorConstants.type] = "string" }, [SchemaValidatorConstants.maxContains] = 3 }
            }
        };
        using (ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition))
        {
            schemaValidator.Init(blitSchemaDefinition);
        }

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [prop] = new DynamicJsonArray { 1, "somestring2" }, }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [prop] = new DynamicJsonArray { 1, 2 } }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError(
                    "The array at 'somepropname' must contain at least 1 items matching the required schema, but no items where found. Schema : {\"type\":\"string\"}",
                    errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [prop] = new DynamicJsonArray { "somestring1", "somestring2", "somestring3", "somestring4" } },
                    out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError(
                    "The array at 'somepropname' must not contain more than 3 items matching the required schema, but 4 matching items were found. schema : {\"type\":\"string\"}",
                    errors);
            });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenRestrictOnArrayContainsWithMinRestriction()
    {
        const string prop = "somepropname";

        var schemaValidator = new SchemaValidator(ContextPool);
        var schemaDefinition = new DynamicJsonValue
        {
            [SchemaValidatorConstants.properties] = new DynamicJsonValue
            {
                [prop] = new DynamicJsonValue { [SchemaValidatorConstants.contains] = new DynamicJsonValue { [SchemaValidatorConstants.type] = "string" }, [SchemaValidatorConstants.minContains] = 3 }
            }
        };
        using (ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition))
        {
            schemaValidator.Init(blitSchemaDefinition);
        }

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [prop] = new DynamicJsonArray { "somestring1", "somestring2", "somestring3" }, }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [prop] = new DynamicJsonArray { "somestring1", "somestring2", "somestring3", "somestring4" }, },
                    out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [prop] = new DynamicJsonArray { 1, 2 } }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError(
                    "The array at 'somepropname' must contain at least 3 items matching the required schema, but no items where found. Schema : {\"type\":\"string\"}",
                    errors);
            });
    }
}
