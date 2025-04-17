using System.Threading.Tasks;
using Raven.Server.SchemaValidation;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using SVC = Raven.Server.SchemaValidation.SchemaValidatorConstants;

namespace FastTests.SchemaValidation;

public class ConstantSchemaValidationTests : SchemaValidationTestsBase
{
    public ConstantSchemaValidationTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenValidateStringConstant()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();

        using var schemaValidator = new SchemaValidator(ContextPool);
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Type] = "object",
            [SVC.Properties] = new DynamicJsonValue
            {
                ["stringProp"] = new DynamicJsonValue { [SVC.Const] = "somevalue" },
            }
        };
        using (ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition))
        {
            schemaValidator.Init(blitSchemaDefinition);
        }

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["stringProp"] = "somevalue" }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["stringProp"] = "someothervalue" }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The value at 'stringProp' must be '\"somevalue\"', but it is '\"someothervalue\"'.", errors);
            });
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenValidateIntConstant()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();

        using var schemaValidator = new SchemaValidator(ContextPool);
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Type] = "object",
            [SVC.Properties] = new DynamicJsonValue
            {
                ["intProp"] = new DynamicJsonValue { [SVC.Const] = 21 },
            }
        };
        using (ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition))
        {
            schemaValidator.Init(blitSchemaDefinition);
        }

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["intProp"] = 21 }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["intProp"] = 21 + 3 }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The value at 'intProp' must be '21', but it is '24'.", errors);
            });
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenValidateDoubleConstant()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();

        using var schemaValidator = new SchemaValidator(ContextPool);
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Type] = "object",
            [SVC.Properties] = new DynamicJsonValue
            {
                ["doubleProp"] = new DynamicJsonValue { [SVC.Const] = 3.14 },
            }
        };
        using (ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition))
        {
            schemaValidator.Init(blitSchemaDefinition);
        }

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["doubleProp"] = 3.14 }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["doubleProp"] = 6.14 }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The value at 'doubleProp' must be '3.14', but it is '6.14'.", errors);
            });
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenValidateObjectConstant()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();

        using var schemaValidator = new SchemaValidator(ContextPool);
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Type] = "object",
            [SVC.Properties] = new DynamicJsonValue
            {
                ["objectProp"] = new DynamicJsonValue { [SVC.Const] = new DynamicJsonValue{["prop"] = 44} }
            }
        };
        using (ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition))
        {
            schemaValidator.Init(blitSchemaDefinition);
        }

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["objectProp"] = new DynamicJsonValue{["prop"] = 44} }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["objectProp"] = new DynamicJsonValue{["anotherprop"] = 44} }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The value at 'objectProp' must be '{\"prop\":44}', but it is '{\"anotherprop\":44}'.", errors);
            },
            () =>
            {
                //TODO Make sure a string object is not valid as an object
                // using var ctx = ReadObject(new DynamicJsonValue { ["objectProp"] = ReadObject(new DynamicJsonValue{["prop"] = 44}).ToString() });
                //
                // Assert.False(schemaValidator.Validate(obj, out var errors));
                // AssertError("The value at 'objectProp' must be '{\"prop\":44}', but it is '{\"prop\": 44}'.", errors);
            });
    }
}
