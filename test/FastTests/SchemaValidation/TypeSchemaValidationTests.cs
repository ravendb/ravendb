using Raven.Server.SchemaValidation;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.SchemaValidation;

public class TypeSchemaValidationTests : SchemaValidationTestsBase
{
    public TypeSchemaValidationTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateObject_ShouldSuccess()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();
        var schemaDefinition = context.ReadObject(new DynamicJsonValue
        {
            ["type"] = "object",
        }, "schema Definition");
        
        var testObj = context.ReadObject(new DynamicJsonValue
        {
            
        }, "test object");

        var schemaValidator = new SchemaValidator();
        schemaValidator.Init(schemaDefinition);
        
        if (schemaValidator.Validate(testObj, out string errors) == false)
            Assert.Fail(errors);
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateNestedObjAndTrue_ShouldSuccess()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();
        var schemaDefinition = context.ReadObject(new DynamicJsonValue
        {
            ["type"] = "object",
            ["properties"] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue
                {
                    ["type"] = "object"
                }
            }
        }, "schema Definition");
        
        var testObj = context.ReadObject(new DynamicJsonValue
        {
            ["prop"] = new DynamicJsonValue()
        }, "test object");

        var schemaValidator = new SchemaValidator();
        schemaValidator.Init(schemaDefinition);
        if (schemaValidator.Validate(testObj, out string errors) == false)
            Assert.Fail(string.Join("\n", errors));
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateNestedObjAndString_ShouldFail()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();
        var schemaDefinition = context.ReadObject(new DynamicJsonValue
        {
            ["type"] = "object",
            ["properties"] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue
                {
                    ["type"] = "object"
                }
            }
        }, "schema Definition");
        
        var testObj = context.ReadObject(new DynamicJsonValue
        {
            ["prop"] = "some string"
        }, "test object");

        var schemaValidator = new SchemaValidator();
        schemaValidator.Init(schemaDefinition);
        Assert.False(schemaValidator.Validate(testObj, out string errors));
        //TODO Check exact error
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateStringPropertyAndTrue_ShouldSuccess()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();
        var schemaDefinition = context.ReadObject(new DynamicJsonValue
        {
            ["type"] = "object",
            ["properties"] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue
                {
                    ["type"] = "string"
                }
            }
        }, "schema Definition");
        
        var testObj = context.ReadObject(new DynamicJsonValue
        {
            ["prop"] = "some string"
        }, "test object");

        var schemaValidator = new SchemaValidator();
        schemaValidator.Init(schemaDefinition);
        if (schemaValidator.Validate(testObj, out string errors) == false)
            Assert.Fail(string.Join("\n", errors));
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateStringPropertyAndObject_ShouldSuccess()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();
        var schemaDefinition = context.ReadObject(new DynamicJsonValue
        {
            ["type"] = "object",
            ["properties"] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue
                {
                    ["type"] = "string"
                }
            }
        }, "schema Definition");
        
        var testObj = context.ReadObject(new DynamicJsonValue
        {
            ["prop"] = new DynamicJsonValue()
        }, "test object");

        var schemaValidator = new SchemaValidator();
        schemaValidator.Init(schemaDefinition);
        Assert.False(schemaValidator.Validate(testObj, out string errors));
        //TODO Check exact error
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateIntPropertyAndTrue_ShouldSuccess()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();
        var schemaDefinition = context.ReadObject(new DynamicJsonValue
        {
            ["type"] = "object",
            ["properties"] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue
                {
                    ["type"] = "integer"
                }
            }
        }, "schema Definition");
        
        var testObj = context.ReadObject(new DynamicJsonValue
        {
            ["prop"] = 1
        }, "test object");

        var schemaValidator = new SchemaValidator();
        schemaValidator.Init(schemaDefinition);
        if (schemaValidator.Validate(testObj, out string errors) == false)
            Assert.Fail(string.Join("\n", errors));
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateIntPropertyAndObject_ShouldFail()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();
        var schemaDefinition = context.ReadObject(new DynamicJsonValue
        {
            ["type"] = "object",
            ["properties"] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue
                {
                    ["type"] = "integer"
                }
            }
        }, "schema Definition");
        
        var testObj = context.ReadObject(new DynamicJsonValue
        {
            ["prop"] = new DynamicJsonValue()
        }, "test object");

        var schemaValidator = new SchemaValidator();
        schemaValidator.Init(schemaDefinition);
        Assert.False(schemaValidator.Validate(testObj, out string errors));
        //TODO Check exact error
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateIntPropertyAndFloat_ShouldFail()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();
        var schemaDefinition = context.ReadObject(new DynamicJsonValue
        {
            ["type"] = "object",
            ["properties"] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue
                {
                    ["type"] = "integer"
                }
            }
        }, "schema Definition");
        
        var testObj = context.ReadObject(new DynamicJsonValue
        {
            ["prop"] = 3.14
        }, "test object");

        var schemaValidator = new SchemaValidator();
        schemaValidator.Init(schemaDefinition);
        Assert.False(schemaValidator.Validate(testObj, out string errors));
        //TODO Check exact error
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateBoolean()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();

        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            ["type"] = "object",
            ["properties"] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue { ["type"] = "boolean" }
            }
        };
        using (var blitSchemaDefinition = ReadObject(schemaDefinition))
        {
            schemaValidator.Init(blitSchemaDefinition);
        }

        Assert.Multiple(() =>
            {
                using var obj = ReadObject(new DynamicJsonValue { ["prop"] = true });

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var obj = ReadObject(new DynamicJsonValue { ["prop"] = false });

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var obj = ReadObject(new DynamicJsonValue { ["prop"] = 17.3 });

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("'prop' should be of type 'boolean' but actual type is 'number'.", errors);
            });
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateNull()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();

        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            ["type"] = "object",
            ["properties"] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue { ["type"] = "null" }
            }
        };
        using (var blitSchemaDefinition = ReadObject(schemaDefinition))
        {
            schemaValidator.Init(blitSchemaDefinition);
        }

        Assert.Multiple(() =>
            {
                using var obj = ReadObject(new DynamicJsonValue { ["prop"] = null });

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var obj = ReadObject(new DynamicJsonValue { ["prop"] = 17.3 });

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("'prop' should be of type 'null' but actual type is 'number'.", errors);
            });
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateObjectOrNull()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();

        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            ["type"] = "object",
            ["properties"] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue { ["type"] = new DynamicJsonArray{"null", "object"} }
            }
        };
        using (var blitSchemaDefinition = ReadObject(schemaDefinition))
        {
            schemaValidator.Init(blitSchemaDefinition);
        }

        Assert.Multiple(() =>
            {
                using var obj = ReadObject(new DynamicJsonValue { ["prop"] = null });

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var obj = ReadObject(new DynamicJsonValue { ["prop"] = new DynamicJsonValue() });

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var obj = ReadObject(new DynamicJsonValue { ["prop"] = 54 });

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("'prop' should be of type 'null' or 'object' but actual type is 'integer'.", errors);
            });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateEmptyArray()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();

        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            ["type"] = "object",
            ["properties"] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue { ["type"] = new DynamicJsonArray{} }
            }
        };
        using (var blitSchemaDefinition = ReadObject(schemaDefinition))
        {
            schemaValidator.Init(blitSchemaDefinition);
        }

        Assert.Multiple(() =>
            {
                using var obj = ReadObject(new DynamicJsonValue { ["prop"] = 54 });

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateEnum()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();

        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            ["type"] = "object",
            ["properties"] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue { ["enum"] = new object[] { 15, "somevalue", new DynamicJsonValue { ["prop"] = 8 } } }
            }
        };
        using (var blitSchemaDefinition = ReadObject(schemaDefinition))
        {
            schemaValidator.Init(blitSchemaDefinition);
        }

        Assert.Multiple(() =>
            {
                using var obj = ReadObject(new DynamicJsonValue { ["prop"] = 15 });

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var obj = ReadObject(new DynamicJsonValue { ["prop"] = "somevalue" });

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var obj = ReadObject(new DynamicJsonValue { ["prop"] = new DynamicJsonValue { ["prop"] = 8 } });

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var obj = ReadObject(new DynamicJsonValue { ["prop"] = "16" });

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The value '16' at 'prop' is not an allowed value. Expected one of: 15, somevalue, {\"prop\":8}.", errors);
            },
            () =>
            {
                using var obj = ReadObject(new DynamicJsonValue { ["prop"] = "someothervalue" });

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The value 'someothervalue' at 'prop' is not an allowed value. Expected one of: 15, somevalue, {\"prop\":8}.", errors);
            },
            () =>
            {
                using var obj = ReadObject(new DynamicJsonValue { ["prop"] = new DynamicJsonValue { ["prop"] = 9 } });

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The value '{\"prop\":9}' at 'prop' is not an allowed value. Expected one of: 15, somevalue, {\"prop\":8}.", errors);
            });
    }
    
    [RavenTheory(RavenTestCategory.JavaScript)]
    [InlineData("invalidType", "The 'type' restriction must be one of the allowed types (null, integer, number, string, boolean, object, array), but found 'invalidType'. Path: 'prop'.")]
    [InlineData(89, "Expected a value of type 'string' for 'type', but received 'integer' of type '89' at path 'prop'.")]
    [InlineData(45.5, "Expected a value of type 'string' for 'type', but received 'number' of type '45.5' at path 'prop'.")]
    [InlineData(true, "Expected a value of type 'string' for 'type', but received 'boolean' of type 'True' at path 'prop'.")]
    public void InvalidSchema_WhenTypeIsNotValid_ShouldThrow(object type, string error)
    {
        using var context = JsonOperationContext.ShortTermSingleUse();

        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            ["properties"] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue { ["type"] = type }
            },
        };
        using (var blitSchemaDefinition = ReadObject(schemaDefinition))
        {
            var e = Assert.Throws<InvalidSchemaValidationDefinitionException>(() => schemaValidator.Init(blitSchemaDefinition));
            AssertError(error, e.Message);
        }
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public void InvalidSchema_WhenTypeIsObject_ShouldThrow()
    {
        var type = new DynamicJsonValue();
        InvalidSchema_WhenTypeIsNotValid_ShouldThrow(type, "Expected a value of type 'string' for 'type', but received 'object' of type '{}' at path 'prop'.");
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public void InvalidSchema_WhenTypeIsNull_ShouldThrow()
    {
        InvalidSchema_WhenTypeIsNotValid_ShouldThrow(null, "Expected a value of type 'string' for 'type', but received 'null' of type '' at path 'prop'.");
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public void InvalidSchema_WhenTypeIsArray_ShouldThrow()
    {
        var type = new DynamicJsonArray { 54 };
        InvalidSchema_WhenTypeIsNotValid_ShouldThrow(type, "Expected a value of type 'string' for 'type', but received 'integer' of type '54' at path 'prop'.");
    }
}
