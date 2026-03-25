using System;
using System.Threading.Tasks;
using Raven.Server.Documents.SchemaValidation;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using SVC = Raven.Server.Documents.SchemaValidation.SchemaValidatorConstants;

namespace FastTests.SchemaValidation;

public class ComplexSchemaValidationTests : SchemaValidationTestsBase
{
    public ComplexSchemaValidationTests(ITestOutputHelper output) : base(output) { }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task PersonObjectWithAddressAndAgeValidation()
    {
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Type] = SchemaValidationHelper.Object,
            [SVC.Properties] = new DynamicJsonValue
            {
                ["name"] = new DynamicJsonValue { [SVC.Type] = SchemaValidationHelper.String },
                ["age"] = new DynamicJsonValue { [SVC.Type] = SchemaValidationHelper.Integer, [SVC.Minimum] = 0 },
                ["address"] = new DynamicJsonValue
                {
                    [SVC.Type] = SchemaValidationHelper.Object,
                    [SVC.Properties] = new DynamicJsonValue
                    {
                        ["city"] = new DynamicJsonValue { [SVC.Type] = SchemaValidationHelper.String },
                        ["zip"] = new DynamicJsonValue { [SVC.Type] = SchemaValidationHelper.String, [SVC.Pattern] = Regex(@"\d{5}") }
                    },
                    [SVC.Required] = new[] { "city", "zip" }
                }
            },
            [SVC.Required] = new[] { "name", "age", "address" }
        };

        var schemaValidator = new SchemaValidator();
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["name"] = "John",
                    ["age"] = 25,
                    ["address"] = new DynamicJsonValue
                    {
                        ["city"] = "NY",
                        ["zip"] = "12345"
                    }
                }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["name"] = "John",
                    ["age"] = 25,
                    ["address"] = new DynamicJsonValue
                    {
                        ["city"] = "NY"
                    }
                }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors), errors);
                AssertError("The required property 'zip' is missing at 'address'.", errors);
            });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task ArrayWithUniqueItemsValidation()
    {
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.PatternProperties] = new DynamicJsonValue
            {
                [Regex("^[a-z]{3,}")] = new DynamicJsonValue
                {
                    [SVC.Type] = "array",
                    [SVC.Items] = new DynamicJsonValue { [SVC.Type] = SchemaValidationHelper.Integer },
                    [SVC.UniqueItems] = true
                }
            }
        };

        var schemaValidator = new SchemaValidator();
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["abc"] = new DynamicJsonArray { 1, 2, 3 },
                    ["1bc"] = 1
                }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["abc"] = 1,
                    ["1bc"] = 1
                }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out string errors));
                AssertError("'abc' should be of type 'array' but actual type is 'integer'.", errors);
            });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task EnumConstraintOnString()
    {
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Properties] = new DynamicJsonValue
            {
                ["color"] = new DynamicJsonValue
                {
                    [SVC.Type] = SchemaValidationHelper.String,
                    ["enum"] = new DynamicJsonArray { "red", "green", "blue" }
                },
            }
        };

        var schemaValidator = new SchemaValidator();
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["color"] = "green" }, out var obj);
                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["color"] = "yellow" }, out var obj);
                Assert.False(schemaValidator.Validate(obj, out string errors));
                AssertError(@"The value '""yellow""' at 'color' is not an allowed value. Expected one of: '""red""', '""green""', '""blue""", errors);
            });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task OneOfConstraint()
    {
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.PatternProperties] = new DynamicJsonValue
            {
                ["^v"] = new DynamicJsonValue
                {
                    [SVC.OneOf] = new DynamicJsonArray
                    {
                        new DynamicJsonValue { [SVC.Type] = SchemaValidationHelper.String },
                        new DynamicJsonValue { [SVC.Type] = SchemaValidationHelper.Integer }
                    }
                }
            }
        };

        var schemaValidator = new SchemaValidator();
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["val"] = 123 }, out var obj);
                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["val"] = new DynamicJsonArray()
                }, out var obj);
                
                Assert.False(schemaValidator.Validate(obj, out string errors));
                AssertError("The value at 'val' does not match any of the schema restrictions, and it must match exactly one.", errors);
            });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task StringPatternValidation()
    {
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.AdditionalProperties] = new DynamicJsonValue
            {
                [SVC.Type] = SchemaValidationHelper.String,
                [SVC.Pattern] = Regex(@"^[a-z]{3}\d{2}$")
            }
        };

        var schemaValidator = new SchemaValidator();
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["val"] = "abc12" }, out var obj);
                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["val"] = "ab12" }, out var obj);
                Assert.False(schemaValidator.Validate(obj, out string errors));
                AssertError("The pattern of the value 'ab12' at 'val' does not match the required pattern '^[a-z]{3}\\d{2}$'.", errors);
            });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task NestedObjectValidation()
    {
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Type] = SchemaValidationHelper.Object,
            [SVC.Properties] = new DynamicJsonValue
            {
                ["settings"] = new DynamicJsonValue
                {
                    [SVC.Type] = SchemaValidationHelper.Object,
                    [SVC.Properties] = new DynamicJsonValue
                    {
                        ["enabled"] = new DynamicJsonValue { [SVC.Type] = "boolean" }
                    },
                    [SVC.Required] = new[] { "enabled" }
                }
            }
        };

        var schemaValidator = new SchemaValidator();
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["settings"] = new DynamicJsonValue { ["enabled"] = true }
                }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["settings"] = new DynamicJsonValue()
                }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out string errors));
                AssertError("The required property 'enabled' is missing at 'settings'.", errors);
            });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task DependentRequiredValidation()
    {
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Type] = SchemaValidationHelper.Object,
            [SVC.DependentRequired] = new DynamicJsonValue
            {
                ["credit_card"] = new[] { "billing_address" }
            }
        };

        var schemaValidator = new SchemaValidator();
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["credit_card"] = "1234-5678-9999",
                    ["billing_address"] = "123 Main St"
                }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["credit_card"] = "1234-5678-9999"
                }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out string errors));
                AssertError("The required property 'billing_address' is missing at ''", errors);
            });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task AllOfWithTypeConstraints()
    {
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.AdditionalProperties] = new DynamicJsonValue
            {
                [SVC.AllOf] = new DynamicJsonArray
                {
                    new DynamicJsonValue { [SVC.Type] = SchemaValidationHelper.Integer },
                    new DynamicJsonValue { [SVC.Minimum] = 10 }
                }
            }
        };

        var schemaValidator = new SchemaValidator();
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["value"] = 15 }, out var obj);
                
                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["value"] = 5 }, out var obj);
                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The value '5' at 'value' should be greater than or equal to 10.", errors);
            });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task ArrayOfObjectsWithRequiredProperties()
    {
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.AdditionalProperties] = new DynamicJsonValue
            {
                [SVC.Type] = SchemaValidationHelper.Array,
                [SVC.Items] = new DynamicJsonValue
                {
                    [SVC.Type] = SchemaValidationHelper.Object,
                    [SVC.Properties] = new DynamicJsonValue
                    {
                        ["id"] = new DynamicJsonValue { [SVC.Type] = SchemaValidationHelper.Integer },
                        ["name"] = new DynamicJsonValue { [SVC.Type] = SchemaValidationHelper.String }
                    },
                    [SVC.Required] = new[] { "id", "name" }
                }
            }
        };

        var schemaValidator = new SchemaValidator();
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["@values"] = new DynamicJsonArray
                    {
                        new DynamicJsonValue { ["id"] = 1, ["name"] = "Item 1" },
                        new DynamicJsonValue { ["id"] = 2, ["name"] = "Item 2" }
                    }
                }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["@values"] = new DynamicJsonArray
                    {
                        new DynamicJsonValue { ["id"] = 1 },
                        new DynamicJsonValue { ["name"] = "Item 2" }
                    }
                }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out string errors));
                AssertError($"The required property 'name' is missing at '@values[0]'.{Environment.NewLine}The required property 'id' is missing at '@values[1]'.", errors);
            });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task AnyOfWithConflictingTypes()
    {
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Properties] = new DynamicJsonValue
            {
                ["value"] = new DynamicJsonValue
                {
                    [SVC.AnyOf] = new DynamicJsonArray
                    {
                        new DynamicJsonValue { [SVC.Type] = SchemaValidationHelper.String },
                        new DynamicJsonValue { [SVC.Type] = "boolean" }
                    }
                },
            }
        };

        var schemaValidator = new SchemaValidator();
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);
        
        await AssertMultipleParallel(
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["value"] = true }, out var obj);
                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["value"] = 42 }, out var obj);
                Assert.False(schemaValidator.Validate(obj, out string errors));
                AssertError("The value at 'value' does not match any of the schema restrictions.", errors);
            });
    }

}
