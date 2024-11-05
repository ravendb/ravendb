using Raven.Server.SchemaValidation;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.SchemaValidation;

public class BasicSchemaValidationTests : SchemaValidationTestsBase
{
    public BasicSchemaValidationTests(ITestOutputHelper output) : base(output)
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

    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateConstant()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();

        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            ["type"] = "object",
            ["properties"] = new DynamicJsonValue
            {
                ["stringProp"] = new DynamicJsonValue { ["const"] = "somevalue" },
                ["intProp"] = new DynamicJsonValue { ["const"] = 21 },
                ["doubleProp"] = new DynamicJsonValue { ["const"] = 3.14 },
                ["objectProp"] = new DynamicJsonValue { ["const"] = new DynamicJsonValue{["prop"] = 44} }
            }
        };
        using (var blitSchemaDefinition = ReadObject(schemaDefinition))
        {
            schemaValidator.Init(blitSchemaDefinition);
        }

        Assert.Multiple(() =>
            {
                using var obj = ReadObject(new DynamicJsonValue { ["stringProp"] = "somevalue" });

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var obj = ReadObject(new DynamicJsonValue { ["intProp"] = 21 });

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var obj = ReadObject(new DynamicJsonValue { ["doubleProp"] = 3.14 });

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var obj = ReadObject(new DynamicJsonValue { ["objectProp"] = new DynamicJsonValue{["prop"] = 44} });

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var obj = ReadObject(new DynamicJsonValue { ["stringProp"] = "someothervalue" });

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The value at 'root.stringProp' must be 'somevalue', but it is 'someothervalue'.", errors);
            },
            () =>
            {
                using var obj = ReadObject(new DynamicJsonValue { ["intProp"] = 21 + 3 });

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The value at 'root.intProp' must be '21', but it is '24'.", errors);
            },
            () =>
            {
                using var obj = ReadObject(new DynamicJsonValue { ["doubleProp"] = 6.14 });

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The value at 'root.doubleProp' must be '3.14', but it is '6.14'.", errors);
            },
            () =>
            {
                using var obj = ReadObject(new DynamicJsonValue { ["objectProp"] = new DynamicJsonValue{["anotherprop"] = 44} });

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The value at 'root.objectProp' must be '{\"prop\":44}', but it is '{\"anotherprop\":44}'.", errors);
            },
            () =>
            {
                //TODO Make sure a string object is not valid as an object
                // using var obj = ReadObject(new DynamicJsonValue { ["objectProp"] = ReadObject(new DynamicJsonValue{["prop"] = 44}).ToString() });
                //
                // Assert.False(schemaValidator.Validate(obj, out var errors));
                // AssertError("The value at 'root.objectProp' must be '{\"prop\":44}', but it is '{\"prop\": 44}'.", errors);
            });
    }
}
