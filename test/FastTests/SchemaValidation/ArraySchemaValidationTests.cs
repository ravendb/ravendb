using System;
using System.Threading.Tasks;
using Raven.Server.Documents.SchemaValidation;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

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
        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue { [SchemaValidatorConstants.Properties] = new DynamicJsonValue { ["prop1"] = new DynamicJsonValue { [SchemaValidatorConstants.UniqueItems] = true } } };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop1"] = new DynamicJsonArray { 1, 2, 3 }, }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop1"] = 1, }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
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

        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            [SchemaValidatorConstants.Properties] = new DynamicJsonValue
            {
                [prop] = new DynamicJsonValue
                {
                    [SchemaValidatorConstants.PrefixItems] = new DynamicJsonArray
                    {
                        new DynamicJsonValue { [SchemaValidatorConstants.Type] = "integer" }, new DynamicJsonValue { [SchemaValidatorConstants.Type] = "string" },
                    }
                }
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [prop] = new DynamicJsonArray { 1 }, }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [prop] = new DynamicJsonArray { 1, "somestring" }, }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [prop] = new DynamicJsonArray { 1 }, }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [prop] = new DynamicJsonArray { 1, "somestring", "additional" }, }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
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

        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            [SchemaValidatorConstants.Properties] = new DynamicJsonValue
            {
                [prop] = new DynamicJsonValue
                {
                    [SchemaValidatorConstants.PrefixItems] = new DynamicJsonArray
                    {
                        new DynamicJsonValue { [SchemaValidatorConstants.Type] = "integer" }, new DynamicJsonValue { [SchemaValidatorConstants.Type] = "string" },
                    },
                    [SchemaValidatorConstants.Items] = false
                }
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [prop] = new DynamicJsonArray { 1, "" }, }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
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

        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            [SchemaValidatorConstants.Properties] = new DynamicJsonValue { [prop] = new DynamicJsonValue { [SchemaValidatorConstants.Items] = new DynamicJsonValue { [SchemaValidatorConstants.Type] = "string" } } }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [prop] = new DynamicJsonArray { "somestring", "somestring2" }, }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [prop] = new DynamicJsonArray { 1, "", 2 } }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError($"'somepropname[0]' should be of type 'string' but actual type is 'integer'.{Environment.NewLine}'somepropname[2]' should be of type 'string' but actual type is 'integer'", errors);
            });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenRestrictOnArrayContains()
    {
        const string prop = "somepropname";

        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            [SchemaValidatorConstants.Properties] = new DynamicJsonValue { [prop] = new DynamicJsonValue { [SchemaValidatorConstants.Contains] = new DynamicJsonValue { [SchemaValidatorConstants.Type] = "string" } } }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [prop] = new DynamicJsonArray { 1, "somestring2" }, }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
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

        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            [SchemaValidatorConstants.Properties] = new DynamicJsonValue
            {
                [prop] = new DynamicJsonValue { [SchemaValidatorConstants.Contains] = new DynamicJsonValue { [SchemaValidatorConstants.Type] = "string" }, [SchemaValidatorConstants.MaxContains] = 3 }
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [prop] = new DynamicJsonArray { 1, "somestring2" }, }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
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

        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            [SchemaValidatorConstants.Properties] = new DynamicJsonValue
            {
                [prop] = new DynamicJsonValue { [SchemaValidatorConstants.Contains] = new DynamicJsonValue { [SchemaValidatorConstants.Type] = "string" }, [SchemaValidatorConstants.MinContains] = 3 }
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [prop] = new DynamicJsonArray { "somestring1", "somestring2", "somestring3" }, }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [prop] = new DynamicJsonArray { "somestring1", "somestring2", "somestring3", "somestring4" }, },
                    out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
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
