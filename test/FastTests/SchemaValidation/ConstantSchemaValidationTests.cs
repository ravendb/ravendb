using Raven.Server.SchemaValidation;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.SchemaValidation;

public class ConstantSchemaValidationTests : SchemaValidationTestsBase
{
    public ConstantSchemaValidationTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateStringConstant()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();

        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            ["type"] = "object",
            ["properties"] = new DynamicJsonValue
            {
                ["stringProp"] = new DynamicJsonValue { ["const"] = "somevalue" },
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
                using var obj = ReadObject(new DynamicJsonValue { ["stringProp"] = "someothervalue" });

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The value at 'stringProp' must be 'somevalue', but it is 'someothervalue'.", errors);
            });
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateIntConstant()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();

        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            ["type"] = "object",
            ["properties"] = new DynamicJsonValue
            {
                ["intProp"] = new DynamicJsonValue { ["const"] = 21 },
            }
        };
        using (var blitSchemaDefinition = ReadObject(schemaDefinition))
        {
            schemaValidator.Init(blitSchemaDefinition);
        }

        Assert.Multiple(() =>
            {
                using var obj = ReadObject(new DynamicJsonValue { ["intProp"] = 21 });

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var obj = ReadObject(new DynamicJsonValue { ["intProp"] = 21 + 3 });

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The value at 'intProp' must be '21', but it is '24'.", errors);
            });
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateDoubleConstant()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();

        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            ["type"] = "object",
            ["properties"] = new DynamicJsonValue
            {
                ["doubleProp"] = new DynamicJsonValue { ["const"] = 3.14 },
            }
        };
        using (var blitSchemaDefinition = ReadObject(schemaDefinition))
        {
            schemaValidator.Init(blitSchemaDefinition);
        }

        Assert.Multiple(() =>
            {
                using var obj = ReadObject(new DynamicJsonValue { ["doubleProp"] = 3.14 });

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var obj = ReadObject(new DynamicJsonValue { ["doubleProp"] = 6.14 });

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The value at 'doubleProp' must be '3.14', but it is '6.14'.", errors);
            });
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateObjectConstant()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();

        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            ["type"] = "object",
            ["properties"] = new DynamicJsonValue
            {
                ["objectProp"] = new DynamicJsonValue { ["const"] = new DynamicJsonValue{["prop"] = 44} }
            }
        };
        using (var blitSchemaDefinition = ReadObject(schemaDefinition))
        {
            schemaValidator.Init(blitSchemaDefinition);
        }

        Assert.Multiple(() =>
            {
                using var obj = ReadObject(new DynamicJsonValue { ["objectProp"] = new DynamicJsonValue{["prop"] = 44} });

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var obj = ReadObject(new DynamicJsonValue { ["objectProp"] = new DynamicJsonValue{["anotherprop"] = 44} });

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The value at 'objectProp' must be '{\"prop\":44}', but it is '{\"anotherprop\":44}'.", errors);
            },
            () =>
            {
                //TODO Make sure a string object is not valid as an object
                // using var obj = ReadObject(new DynamicJsonValue { ["objectProp"] = ReadObject(new DynamicJsonValue{["prop"] = 44}).ToString() });
                //
                // Assert.False(schemaValidator.Validate(obj, out var errors));
                // AssertError("The value at 'objectProp' must be '{\"prop\":44}', but it is '{\"prop\": 44}'.", errors);
            });
    }
}
