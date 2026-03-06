using System.Threading.Tasks;
using Raven.Server.Documents.SchemaValidation;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using SVC = Raven.Server.Documents.SchemaValidation.SchemaValidatorConstants;

namespace FastTests.SchemaValidation;

public class NumberRulesSchemaValidationTests : SchemaValidationTestsBase
{
    // ReSharper disable once ConvertToPrimaryConstructor
    public NumberRulesSchemaValidationTests(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenValidateMinimum()
    {
        const string longProp = "longProp";
        const string doubleProp = "doubleProp";

        var schemaValidator = new SchemaValidator();

        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Type] = "object",
            [SVC.Properties] = new DynamicJsonValue
            {
                [longProp] = new DynamicJsonValue
                {
                    [SVC.Minimum] = 0
                },
                [doubleProp] = new DynamicJsonValue
                {
                    [SVC.Minimum] = 0.5
                }
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);
        
        await AssertMultipleParallel(() =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                [longProp] = 0
            }, out var obj);
            
            Assert.True(schemaValidator.Validate(obj, out var errors), errors);
        },
        () =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                [longProp] = -1
            }, out var obj);
            
            Assert.False(schemaValidator.Validate(obj, out string errors));
            AssertError("The value '-1' at 'longProp' should be greater than or equal to 0.", errors);
        },
        () =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                [longProp] = -1.5
            }, out var obj);
            
            Assert.False(schemaValidator.Validate(obj, out string errors));
            AssertError("The value '-1.5' at 'longProp' should be greater than or equal to 0.", errors);
        },
        () =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                [doubleProp] = 0.5
            }, out var obj);
            
            Assert.True(schemaValidator.Validate(obj, out var errors), errors);
        },
        () =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                [doubleProp] = -1
            }, out var obj);
            
            Assert.False(schemaValidator.Validate(obj, out string errors));
            AssertError("The value '-1' at 'doubleProp' should be greater than or equal to 0.5.", errors);
        },
        () =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                [doubleProp] = -1.5
            }, out var obj);
            
            Assert.False(schemaValidator.Validate(obj, out string errors));
            AssertError("The value '-1.5' at 'doubleProp' should be greater than or equal to 0.5.", errors);
        });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenValidateExclusiveMinimum()
    {
        const string longProp = "longProp";
        const string doubleProp = "doubleProp";

        var schemaValidator = new SchemaValidator();

        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Type] = "object",
            [SVC.Properties] = new DynamicJsonValue
            {
                [longProp] = new DynamicJsonValue
                {
                    [SVC.Minimum] = 0, 
                    [SVC.ExclusiveMinimum] = true
                },
                [doubleProp] = new DynamicJsonValue
                {
                    [SVC.Minimum] = 0.5, 
                    [SVC.ExclusiveMinimum] = true
                }
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [longProp] = 1 }, out var obj);

            Assert.True(schemaValidator.Validate(obj, out var errors), errors);
        },
        () =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [longProp] = 0 }, out var obj);

            Assert.False(schemaValidator.Validate(obj, out string errors));
            AssertError("The value '0' at 'longProp' should be greater than 0.", errors);
        },
        () =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [longProp] = -0.5 }, out var obj);

            Assert.False(schemaValidator.Validate(obj, out string errors));
            AssertError("The value '-0.5' at 'longProp' should be greater than 0.", errors);
        },
        () =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [doubleProp] = 1.5 }, out var obj);

            Assert.True(schemaValidator.Validate(obj, out var errors), errors);
        },
        () =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [doubleProp] = 0 }, out var obj);

            Assert.False(schemaValidator.Validate(obj, out string errors));
            AssertError("The value '0' at 'doubleProp' should be greater than 0.5.", errors);
        },
        () =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { [doubleProp] = -1.5 }, out var obj);

            Assert.False(schemaValidator.Validate(obj, out string errors));
            AssertError("The value '-1.5' at 'doubleProp' should be greater than 0.5.", errors);
        });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenValidateMaximum()
    {
        const string longProp = "longProp";
        const string doubleProp = "doubleProp";

        var schemaValidator = new SchemaValidator();

        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Type] = "object",
            [SVC.Properties] = new DynamicJsonValue
            {
                [longProp] = new DynamicJsonValue
                {
                    [SVC.Maximum] = 0
                }, 
                [doubleProp] = new DynamicJsonValue
                {
                    [SVC.Maximum] = 0.5
                },
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                [longProp] = 0
            }, out var obj);
            Assert.True(schemaValidator.Validate(obj, out var errors), errors);
        },
        () =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                [longProp] = 1
            }, out var obj);

            Assert.False(schemaValidator.Validate(obj, out string errors));
            AssertError("The value '1' at 'longProp' should be less than or equal to 0.", errors);
        },
        () =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                [longProp] = 0.5
            }, out var obj);

            Assert.False(schemaValidator.Validate(obj, out string errors));
            AssertError("The value '0.5' at 'longProp' should be less than or equal to 0.", errors);
        },
        () =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                [doubleProp] = 0.5
            }, out var obj);

            Assert.True(schemaValidator.Validate(obj, out var errors), errors);
        },
        () =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                [doubleProp] = 1
            }, out var obj);

            Assert.False(schemaValidator.Validate(obj, out string errors));
            AssertError("The value '1' at 'doubleProp' should be less than or equal to 0.5.", errors);
        },
        () =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                [doubleProp] = 1.5
            }, out var obj);

            Assert.False(schemaValidator.Validate(obj, out string errors));
            AssertError("The value '1.5' at 'doubleProp' should be less than or equal to 0.5.", errors);
        });
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenValidateExclusiveMaximum()
    {
        const string longProp = "longProp";
        const string doubleProp = "doubleProp";

        var schemaValidator = new SchemaValidator();

        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Type] = "object",
            [SVC.Properties] = new DynamicJsonValue
            {
                [longProp] = new DynamicJsonValue
                {
                    [SVC.Maximum] = 0,
                    [SVC.ExclusiveMaximum] = true
                },
                [doubleProp] = new DynamicJsonValue
                {
                    [SVC.Maximum] = 0.5,
                    [SVC.ExclusiveMaximum] = true
                }
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);
        
        
        await AssertMultipleParallel(() =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                [longProp] = -1
            }, out var obj);

            Assert.True(schemaValidator.Validate(obj, out var errors), errors);
        },
        () =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                [longProp] = 0
            }, out var obj);

            Assert.False(schemaValidator.Validate(obj, out string errors));
            AssertError("The value '0' at 'longProp' should be less than 0.", errors);
        },
        () =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                [longProp] = 0.5
            }, out var obj);

            Assert.False(schemaValidator.Validate(obj, out string errors));
            AssertError("The value '0.5' at 'longProp' should be less than 0.", errors);
        },
        () =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                [doubleProp] = -0.5
            }, out var obj);

            Assert.True(schemaValidator.Validate(obj, out var errors), errors);
        },
        () =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                [doubleProp] = 1
            }, out var obj);

            Assert.False(schemaValidator.Validate(obj, out string errors));
            AssertError("The value '1' at 'doubleProp' should be less than 0.5.", errors);
        },
        () =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                [doubleProp] = 0.5
            }, out var obj);

            Assert.False(schemaValidator.Validate(obj, out string errors));
            AssertError("The value '0.5' at 'doubleProp' should be less than 0.5.", errors);
        });
    }
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenValidateMultipleOf()
    {
        const string longProp = "longProp";
        const string doubleProp = "doubleProp";

        var schemaValidator = new SchemaValidator();

        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Type] = "object",
            [SVC.Properties] = new DynamicJsonValue
            {
                [longProp] = new DynamicJsonValue
                {
                    [SVC.MultipleOf] = 3
                },
                [doubleProp] = new DynamicJsonValue
                {
                    [SVC.MultipleOf] = 0.6
                }
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);
        
        
        await AssertMultipleParallel(() =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                [longProp] = 3
            }, out var obj);

            Assert.True(schemaValidator.Validate(obj, out var errors), errors);
        }, 
        () =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                [longProp] = 6
            }, out var obj);

            Assert.True(schemaValidator.Validate(obj, out var errors), errors);
        },
        () =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                [longProp] = -9
            }, out var obj);

            Assert.True(schemaValidator.Validate(obj, out var errors), errors);
        },
        () =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                [longProp] = 4
            }, out var obj);

            Assert.False(schemaValidator.Validate(obj, out string errors));
            AssertError("The value '4' at 'longProp' should be a multiple of 3.", errors);
        },
        () =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                [longProp] = 4.5
            }, out var obj);

            Assert.False(schemaValidator.Validate(obj, out string errors));
            AssertError("The value '4.5' at 'longProp' should be a multiple of 3.", errors);
        },
        () =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                [doubleProp] = 0.6
            }, out var obj);

            Assert.True(schemaValidator.Validate(obj, out var errors), errors);
        },
        () =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                [doubleProp] = 6
            }, out var obj);

            Assert.True(schemaValidator.Validate(obj, out var errors), errors);
        },
        () =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                [doubleProp] = -1.8
            }, out var obj);

            Assert.True(schemaValidator.Validate(obj, out var errors), errors);
        },
        () =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                [doubleProp] = 0.5
            }, out var obj);

            Assert.False(schemaValidator.Validate(obj, out string errors));
            AssertError("The value '0.5' at 'doubleProp' should be a multiple of 0.6.", errors);
        },
        () =>
        {
            using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
            {
                [doubleProp] = 1
            }, out var obj);

            Assert.False(schemaValidator.Validate(obj, out string errors));
            AssertError("The value '1' at 'doubleProp' should be a multiple of 0.6.", errors);
        });
    }
}
