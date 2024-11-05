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
                AssertError("root.prop should be of type Boolean but actual type is Number", errors);
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
                AssertError("The value '16' at 'root.prop' is not an allowed value. Expected one of: 15, somevalue, {\"prop\":8}.", errors);
            },
            () =>
            {
                using var obj = ReadObject(new DynamicJsonValue { ["prop"] = "someothervalue" });

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The value 'someothervalue' at 'root.prop' is not an allowed value. Expected one of: 15, somevalue, {\"prop\":8}.", errors);
            },
            () =>
            {
                using var obj = ReadObject(new DynamicJsonValue { ["prop"] = new DynamicJsonValue { ["prop"] = 9 } });

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The value '{\"prop\":9}' at 'root.prop' is not an allowed value. Expected one of: 15, somevalue, {\"prop\":8}.", errors);
            });
    }
}
