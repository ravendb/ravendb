using System.Threading.Tasks;
using Raven.Server.Documents.SchemaValidation;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using SVC = Raven.Server.Documents.SchemaValidation.SchemaValidatorConstants;

namespace FastTests.SchemaValidation;

public class TypeSchemaValidationTests : SchemaValidationTestsBase
{
    // ReSharper disable once ConvertToPrimaryConstructor
    public TypeSchemaValidationTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateObject_ShouldSuccess()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();
        var schemaValidator = new SchemaValidator();

        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Type] = "object",
        };
        using var blitSchemaDefinition = context.ReadObject(schemaDefinition, "schema Definition");
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);
        using var obj = context.ReadObject(new DynamicJsonValue(), "test object");
        Assert.True(schemaValidator.Validate(obj, out var errors), errors);
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateNestedObjAndTrue_ShouldSuccess()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();
        var schemaValidator = new SchemaValidator();

        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Type] = "object",
            [SVC.Properties] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue
                {
                    [SVC.Type] = "object"
                }
            }
        };
        using var blitSchemaDefinition = context.ReadObject(schemaDefinition, "schema Definition");
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);
        
        using var obj = context.ReadObject(new DynamicJsonValue
        {
            ["prop"] = new DynamicJsonValue()
        }, "test object");

        Assert.True(schemaValidator.Validate(obj, out var errors), errors);
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateNestedObjAndString_ShouldFail()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();
        var schemaValidator = new SchemaValidator();

        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Type] = "object",
            [SVC.Properties] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue
                {
                    [SVC.Type] = "object"
                }
            }
        };
        using var blitSchemaDefinition = context.ReadObject(schemaDefinition, "schema Definition");
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);
        
        using var testObj = context.ReadObject(new DynamicJsonValue
        {
            ["prop"] = "some string"
        }, "test object");

        Assert.False(schemaValidator.Validate(testObj, out string errors));
        AssertError("'prop' should be of type 'object' but actual type is 'string'.", errors);
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateStringPropertyAndTrue_ShouldSuccess()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();
        var schemaValidator = new SchemaValidator();
        
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Type] = "object",
            [SVC.Properties] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue
                {
                    [SVC.Type] = "string"
                }
            }
        };
        using var blitSchemaDefinition = context.ReadObject(schemaDefinition, "schema Definition");
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);
        using var testObj = context.ReadObject(new DynamicJsonValue
        {
            ["prop"] = "some string"
        }, "test object");

        Assert.True(schemaValidator.Validate(testObj, out var errors), errors);
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateStringPropertyAndObject_ShouldSuccess()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();
        var schemaValidator = new SchemaValidator();

        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Type] = "object",
            [SVC.Properties] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue
                {
                    [SVC.Type] = "string"
                }
            }
        };
        using var blitSchemaDefinition = context.ReadObject(schemaDefinition, "schema Definition");
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);
        using var obj = context.ReadObject(new DynamicJsonValue
        {
            ["prop"] = new DynamicJsonValue()
        }, "test object");

        Assert.False(schemaValidator.Validate(obj, out string errors));
        AssertError("'prop' should be of type 'string' but actual type is 'object'.", errors);
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateIntPropertyAndTrue_ShouldSuccess()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();
        var schemaValidator = new SchemaValidator();

        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Type] = "object",
            [SVC.Properties] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue
                {
                    [SVC.Type] = "integer"
                }
            }
        };
        using var blitSchemaDefinition = context.ReadObject(schemaDefinition, "schema Definition");
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);
        using var obj = context.ReadObject(new DynamicJsonValue
        {
            ["prop"] = 1
        }, "test object");

        Assert.True(schemaValidator.Validate(obj, out var errors), errors);
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateIntPropertyAndObject_ShouldFail()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();
        var schemaValidator = new SchemaValidator();

        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Type] = "object",
            [SVC.Properties] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue
                {
                    [SVC.Type] = "integer"
                }
            }
        };
        using var blitSchemaDefinition = context.ReadObject(schemaDefinition, "schema Definition");
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);
        var obj = context.ReadObject(new DynamicJsonValue
        {
            ["prop"] = new DynamicJsonValue()
        }, "test object");

        Assert.False(schemaValidator.Validate(obj, out string errors));
        AssertError("'prop' should be of type 'integer' but actual type is 'object'.", errors);
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateIntPropertyAndFloat_ShouldFail()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();
        var schemaValidator = new SchemaValidator();

        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Type] = "object",
            [SVC.Properties] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue
                {
                    [SVC.Type] = "integer"
                }
            }
        };
        using var blitSchemaDefinition = context.ReadObject(schemaDefinition, "schema Definition");
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);
        using var testObj = context.ReadObject(new DynamicJsonValue
        {
            ["prop"] = 3.14
        }, "test object");

        Assert.False(schemaValidator.Validate(testObj, out string errors));
        AssertError("'prop' should be of type 'integer' but actual type is 'number'.", errors);
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenValidateBoolean()
    {
        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Type] = "object",
            [SVC.Properties] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue { [SVC.Type] = "boolean" }
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop"] = true }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop"] = false }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop"] = 17.3 }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("'prop' should be of type 'boolean' but actual type is 'number'.", errors);
            });
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenValidateNull()
    {
        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Type] = "object",
            [SVC.Properties] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue { [SVC.Type] = "null" }
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop"] = null }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop"] = 17.3 }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("'prop' should be of type 'null' but actual type is 'number'.", errors);
            });
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenValidateArray()
    {
        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Type] = "object",
            [SVC.Properties] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue { [SVC.Type] = "array" }
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop"] = new DynamicJsonArray() }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop"] = new DynamicJsonArray{"somevalue"} }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop"] = "notarray" }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("'prop' should be of type 'array' but actual type is 'string'.", errors);
            });
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenValidateObjectOrNull()
    {
        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Type] = "object",
            [SVC.Properties] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue { [SVC.Type] = new DynamicJsonArray{"null", "object"} }
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop"] = null }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop"] = new DynamicJsonValue() }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop"] = 54 }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("'prop' should be of type 'null' or 'object' but actual type is 'integer'.", errors);
            });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenTypeRuleIsEmptyArray_ShouldAllowAllTypes()
    {
        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Type] = "object",
            [SVC.Properties] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue { [SVC.Type] = new DynamicJsonArray{} }
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop"] = 54 }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenValidateEnum()
    {
        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Type] = "object",
            [SVC.Properties] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue { [SVC.Enum] = new object[] { 15, "somevalue", new DynamicJsonValue { ["prop"] = 8 } } }
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop"] = 15 }, out var obj);
            
                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop"] = "somevalue" }, out var obj);
            
                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop"] = new DynamicJsonValue { ["prop"] = 8 } }, out var obj);
            
                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop"] = "16" }, out var obj);
            
                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The value '\"16\"' at 'prop' is not an allowed value. Expected one of: '15', '\"somevalue\"', '{\"prop\":8}'.", errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop"] = "someothervalue" }, out var obj);
            
                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The value '\"someothervalue\"' at 'prop' is not an allowed value. Expected one of: '15', '\"somevalue\"', '{\"prop\":8}'.", errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop"] = new DynamicJsonValue { ["prop"] = 9 } }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The value '{\"prop\":9}' at 'prop' is not an allowed value. Expected one of: '15', '\"somevalue\"', '{\"prop\":8}'.", errors);
            });
    }
    
    [RavenTheory(RavenTestCategory.JavaScript)]
    [InlineData("invalidType", "The 'type' restriction must be one of the allowed types ('null', 'integer', 'number', 'string', 'boolean', 'object', 'array'), but found 'invalidType'. Schema path: '#/properties/prop/type'.")]
    [InlineData(89, "Expected a value of type 'string' for 'type', but received 'integer' of type '89' at path '#/properties/prop/type'.")]
    [InlineData(45.5, "Expected a value of type 'string' for 'type', but received 'number' of type '45.5' at path '#/properties/prop/type'.")]
    [InlineData(true, "Expected a value of type 'string' for 'type', but received 'boolean' of type 'True' at path '#/properties/prop/type'.")]
    public void InvalidSchema_WhenTypeIsNotValid_ShouldThrow(object type, string error)
    {
        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Properties] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue { [SVC.Type] = type }
            },
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        {
            var e = Assert.Throws<InvalidSchemaValidationDefinitionException>(() => schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings));
            AssertError(error, e.Message);
        }
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public void InvalidSchema_WhenTypeIsObject_ShouldThrow()
    {
        var type = new DynamicJsonValue();
        InvalidSchema_WhenTypeIsNotValid_ShouldThrow(type, "Expected a value of type 'string' for 'type', but received 'object' of type '{}' at path '#/properties/prop/type'.");
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public void InvalidSchema_WhenTypeIsNull_ShouldThrow()
    {
        InvalidSchema_WhenTypeIsNotValid_ShouldThrow(null, "Expected a value of type 'string' for 'type', but received 'null' of type '' at path '#/properties/prop/type'.");
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public void InvalidSchema_WhenTypeRuleIsArrayOfInt_ShouldThrow()
    {
        var type = new DynamicJsonArray { 54 };
        InvalidSchema_WhenTypeIsNotValid_ShouldThrow(type, "Expected a value of type 'string' for 'type', but received 'integer' of type '54' at path '#/properties/prop/type'.");
    }
}
