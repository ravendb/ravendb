using System;
using System.Threading.Tasks;
using Raven.Server.Documents.SchemaValidation;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using SVC = Raven.Server.Documents.SchemaValidation.SchemaValidatorConstants;

namespace FastTests.SchemaValidation;

public class ConstantSchemaValidationTests : SchemaValidationTestsBase
{
    public ConstantSchemaValidationTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenValidateStringConstant()
    {
        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Type] = "object",
            [SVC.Properties] = new DynamicJsonValue
            {
                ["stringProp"] = new DynamicJsonValue { [SVC.Const] = "somevalue" },
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["stringProp"] = "somevalue" }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
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
        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Type] = "object",
            [SVC.Properties] = new DynamicJsonValue
            {
                ["intProp"] = new DynamicJsonValue { [SVC.Const] = 21 },
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["intProp"] = 21 }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["intProp"] = 21.0 }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
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
        using IDisposable culture = CultureHelper.EnsureInvariantCulture();
        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Type] = "object",
            [SVC.Properties] = new DynamicJsonValue
            {
                ["doubleProp"] = new DynamicJsonValue { [SVC.Const] = 3.14 },
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["doubleProp"] = 3.14 }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
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
        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Type] = "object",
            [SVC.Properties] = new DynamicJsonValue
            {
                ["objectProp"] = new DynamicJsonValue { [SVC.Const] = new DynamicJsonValue{["prop"] = 44} }
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["objectProp"] = new DynamicJsonValue{["prop"] = 44} }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["objectProp"] = new DynamicJsonValue{["anotherprop"] = 44} }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The value at 'objectProp' must be '{\"prop\":44}', but it is '{\"anotherprop\":44}'.", errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["objectProp"] = "{\"prop\":44}" }, out var obj);
                
                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The value at 'objectProp' must be '{\"prop\":44}', but it is '\"{\"prop\":44}\"'.", errors);
            });
    }
}
