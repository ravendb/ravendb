using System.Threading.Tasks;
using Raven.Server.SchemaValidation;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using SVC = Raven.Server.SchemaValidation.SchemaValidatorConstants;

namespace FastTests.SchemaValidation;

public class ConditionalSchemaValidationTests : SchemaValidationTestsBase
{
    // ReSharper disable once ConvertToPrimaryConstructor
    public ConditionalSchemaValidationTests(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenValidateConditionalSchema()
    {
        const string ifProp = "ifProp";
        const string thenProp = "thenProp";
        const string elseProp = "elseProp";

        var schemaValidator = new SchemaValidator(ContextPool);
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.@if] = new DynamicJsonValue
            {
                [SVC.required] = new []{ifProp}
            },
            [SVC.then] = new DynamicJsonValue
            {
                [SVC.required] = new []{thenProp}
            },
            [SVC.@else] = new DynamicJsonValue
            {
                [SVC.required] = new []{elseProp}
            }
        };

        using (ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition))
        {
            schemaValidator.Init(blitSchemaDefinition);
        }

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    [ifProp] = "123",
                    [thenProp] = "123"
                }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    [elseProp] = "123"
                }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["anotherProp"] = "123"
                }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The required property 'elseProp' is missing at ''.", errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    [ifProp] = "123",
                }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The required property 'thenProp' is missing at ''.", errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The required property 'elseProp' is missing at ''.", errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["anotherProp"] = "123",
                }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The required property 'elseProp' is missing at ''.", errors);
            });
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenRestrictOnDependentRequired()
    {
        var schemaValidator = new SchemaValidator(ContextPool);
        var schemaDefinition = new DynamicJsonValue { [SVC.dependentRequired] = new DynamicJsonValue { ["prop1"] = new[] { "prop2", "prop3" } } };
        using (ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition))
        {
            schemaValidator.Init(blitSchemaDefinition);
        }

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["anotherPropName"] = "somevalue" }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop1"] = "somevalue1", ["prop2"] = "somevalue2", ["prop3"] = "somevalue3", }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop1"] = "somevalue1", }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The required property 'prop2' is missing at ''", errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop1"] = "somevalue1", ["prop2"] = "somevalue1", }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The required property 'prop3' is missing at ''", errors);
            });
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenRestrictOnDependentSchemas()
    {
        var schemaValidator = new SchemaValidator(ContextPool);
        var schemaDefinition = new DynamicJsonValue 
        { 
            [SVC.dependentSchemas] = new DynamicJsonValue 
            { 
                ["prop1"] = new DynamicJsonValue
                {
                    [SVC.properties] = new DynamicJsonValue
                    {
                        ["prop2"] = new DynamicJsonValue
                        {
                            [SVC.type] = "string"
                        }
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
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["anotherPropName"] = "somevalue", ["prop2"] = 1 }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop1"] = "somevalue", ["prop2"] = "1" }, out var obj);

                if (schemaValidator.Validate(obj, out string errors) == false)
                    Assert.Fail(string.Join("\n", errors));
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop1"] = "somevalue", ["prop2"] = 1 }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("'prop2' should be of type 'string' but actual type is 'integer'.", errors);
            });
    }
}
