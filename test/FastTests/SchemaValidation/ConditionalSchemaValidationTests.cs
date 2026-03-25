using System.Threading.Tasks;
using Raven.Server.Documents.SchemaValidation;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using SVC = Raven.Server.Documents.SchemaValidation.SchemaValidatorConstants;

namespace FastTests.SchemaValidation;

public class ConditionalSchemaValidationTests : SchemaValidationTestsBase
{
    // ReSharper disable once ConvertToPrimaryConstructor
    public ConditionalSchemaValidationTests(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenValidateConditionalSchemaOnObject()
    {
        const string ifProp = "ifProp";
        const string thenProp = "thenProp";
        const string elseProp = "elseProp";

        var schemaValidator = new SchemaValidator();
        
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.If] = new DynamicJsonValue
            {
                [SVC.Required] = new []{ifProp}
            },
            [SVC.Then] = new DynamicJsonValue
            {
                [SVC.Required] = new []{thenProp}
            },
            [SVC.Else] = new DynamicJsonValue
            {
                [SVC.Required] = new []{elseProp}
            }
        };

        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    [ifProp] = "123",
                    [thenProp] = "123"
                }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    [elseProp] = "123"
                }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
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
    public async Task SchemaValidation_WhenValidateConditionalSchemaOnString()
    {
        const string stringProp = "stringProp";

        var schemaValidator = new SchemaValidator();

        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Properties] = new DynamicJsonValue
            {
                [stringProp] = new DynamicJsonValue
                {
                    [SVC.If] = new DynamicJsonValue
                    {
                        [SVC.Type] = "string"
                    },
                    [SVC.Then] = new DynamicJsonValue
                    {
                        [SVC.MaxLength] = 5
                    }
                },
            }
        };

        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["stringProp"] = "123",
                }, out var obj);
            
                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    [stringProp] = "123456789",
                }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The length of the value '123456789' at 'stringProp' should not exceed 5, but its actual length is 9.", errors);
            });
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenRestrictOnDependentRequired()
    {
        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue { [SVC.DependentRequired] = new DynamicJsonValue { ["prop1"] = new[] { "prop2", "prop3" } } };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["anotherPropName"] = "somevalue" }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop1"] = "somevalue1", ["prop2"] = "somevalue2", ["prop3"] = "somevalue3", }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
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
        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue 
        { 
            [SVC.DependentSchemas] = new DynamicJsonValue 
            { 
                ["prop1"] = new DynamicJsonValue
                {
                    [SVC.Properties] = new DynamicJsonValue
                    {
                        ["prop2"] = new DynamicJsonValue
                        {
                            [SVC.Type] = "string"
                        }
                    }
                } 
            } 
        };
        
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["anotherPropName"] = "somevalue", ["prop2"] = 1 }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop1"] = "somevalue", ["prop2"] = "1" }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop1"] = "somevalue", ["prop2"] = 1 }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("'prop2' should be of type 'string' but actual type is 'integer'.", errors);
            });
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenRestrictObjectOnNot()
    {
        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue 
        { 
            [SVC.Not] = new DynamicJsonValue 
            { 
                [SVC.Required] = new DynamicJsonArray
                {
                    "prop"
                } 
            } 
        };
        
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["anotherPropName"] = 1 }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue {  }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop"] = "somevalue" }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The value at '' is invalid because it matches a `not` schema.", errors);
            });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenRestrictStringOnNot()
    {
        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue 
        { 
            [SVC.Properties] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue
                {
                    [SVC.Not] = new DynamicJsonValue 
                    { 
                        [SVC.MinLength] = 5
                    } 
                }
            }
        };
        
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop"] = "123" }, out var obj);
            
                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["prop"] = "1234567" }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The value at 'prop' is invalid because it matches a `not` schema.", errors);
            });
    }
}
