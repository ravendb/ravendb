using Raven.Server.SchemaValidation;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.SchemaValidation;

public class NumberRulesSchemaValidationTests : SchemaValidationTestsBase
{
    // ReSharper disable once ConvertToPrimaryConstructor
    public NumberRulesSchemaValidationTests(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateMinimum()
    {
        const string longProp = "longProp";
        const string doubleProp = "doubleProp";

        var schemaValidator = new SchemaValidator();

        using (var schemaDefinition = ReadObject(new DynamicJsonValue
        {
           ["type"] = "object",
           ["properties"] = new DynamicJsonValue
           {
               [longProp] = new DynamicJsonValue
               {
                   ["minimum"] = 0
               },
               [doubleProp] = new DynamicJsonValue
               {
                   ["minimum"] = 0.5
               }
           }
        }))
        {
            schemaValidator.Init(schemaDefinition);
        }
        
        Assert.Multiple(() =>
        {
            using var validObj = ReadObject(new DynamicJsonValue
            {
                [longProp] = 0
            });
            
            if (schemaValidator.Validate(validObj, out string errors) == false)
                Assert.Fail(string.Join("\n", errors));
        },
        () =>
        {
            using var invalidObj = ReadObject(new DynamicJsonValue
            {
                [longProp] = -1
            });
            
            Assert.False(schemaValidator.Validate(invalidObj, out string errors));
            AssertError("The value '-1' at 'root.longProp' should be greater than or equal to 0.", errors);
        },
        () =>
        {
            using var invalidObj = ReadObject(new DynamicJsonValue
            {
                [longProp] = -1.5
            });
            
            Assert.False(schemaValidator.Validate(invalidObj, out string errors));
            AssertError("The value '-1.5' at 'root.longProp' should be greater than or equal to 0.", errors);
        },
        () =>
        {
            using var validObj = ReadObject(new DynamicJsonValue
            {
                [doubleProp] = 0.5
            });
            
            if (schemaValidator.Validate(validObj, out string errors) == false)
                Assert.Fail(string.Join("\n", errors));
        },
        () =>
        {
            using var invalidObj = ReadObject(new DynamicJsonValue
            {
                [doubleProp] = -1
            });
            
            Assert.False(schemaValidator.Validate(invalidObj, out string errors));
            AssertError("The value '-1' at 'root.doubleProp' should be greater than or equal to 0.5.", errors);
        },
        () =>
        {
            using var invalidObj = ReadObject(new DynamicJsonValue
            {
                [doubleProp] = -1.5
            });
            
            Assert.False(schemaValidator.Validate(invalidObj, out string errors));
            AssertError("The value '-1.5' at 'root.doubleProp' should be greater than or equal to 0.5.", errors);
        });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateExclusiveMinimum()
    {
        const string longProp = "longProp";
        const string doubleProp = "doubleProp";

        var schemaValidator = new SchemaValidator();

        using (var schemaDefinition = ReadObject(new DynamicJsonValue
        {
            ["type"] = "object",
            ["properties"] = new DynamicJsonValue
            {
               [longProp] = new DynamicJsonValue
               {
                   ["minimum"] = 0, 
                   ["exclusiveMinimum"] = true
               },
               [doubleProp] = new DynamicJsonValue
               {
                   ["minimum"] = 0.5, 
                   ["exclusiveMinimum"] = true
               }
            }
        }))
        {
            schemaValidator.Init(schemaDefinition);
        }

        Assert.Multiple(() =>
        {
            using var validObj = ReadObject(new DynamicJsonValue { [longProp] = 1 });

            if (schemaValidator.Validate(validObj, out string errors) == false)
                Assert.Fail(string.Join("\n", errors));
        },
        () =>
        {
            using var invalidObj = ReadObject(new DynamicJsonValue { [longProp] = 0 });

            Assert.False(schemaValidator.Validate(invalidObj, out string errors));
            AssertError("The value '0' at 'root.longProp' should be greater than 0.", errors);
        },
        () =>
        {
            using var invalidObj = ReadObject(new DynamicJsonValue { [longProp] = -0.5 });

            Assert.False(schemaValidator.Validate(invalidObj, out string errors));
            AssertError("The value '-0.5' at 'root.longProp' should be greater than 0.", errors);
        },
        () =>
        {
            using var validObj = ReadObject(new DynamicJsonValue { [doubleProp] = 1.5 });

            if (schemaValidator.Validate(validObj, out string errors) == false)
                Assert.Fail(string.Join("\n", errors));
        },
        () =>
        {
            using var invalidObj = ReadObject(new DynamicJsonValue { [doubleProp] = 0 });

            Assert.False(schemaValidator.Validate(invalidObj, out string errors));
            AssertError("The value '0' at 'root.doubleProp' should be greater than 0.5.", errors);
        },
        () =>
        {
            using var invalidObj = ReadObject(new DynamicJsonValue { [doubleProp] = -1.5 });

            Assert.False(schemaValidator.Validate(invalidObj, out string errors));
            AssertError("The value '-1.5' at 'root.doubleProp' should be greater than 0.5.", errors);
        });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateMaximum()
    {
        const string longProp = "longProp";
        const string doubleProp = "doubleProp";

        var schemaValidator = new SchemaValidator();

        using (var schemaDefinition = ReadObject(new DynamicJsonValue
        {
           ["type"] = "object",
           ["properties"] = new DynamicJsonValue
           {
               [longProp] = new DynamicJsonValue
               {
                   ["maximum"] = 0
               }, 
               [doubleProp] = new DynamicJsonValue
               {
                   ["maximum"] = 0.5
               },
           }
        }))
        {
            schemaValidator.Init(schemaDefinition);
        }

        Assert.Multiple(() =>
        {
            var validObj = ReadObject(new DynamicJsonValue
            {
                [longProp] = 0
            });
            if (schemaValidator.Validate(validObj, out string errors) == false)
                Assert.Fail(string.Join("\n", errors));
        },
        () =>
        {
            var invalidObj = ReadObject(new DynamicJsonValue
            {
                [longProp] = 1
            });

            Assert.False(schemaValidator.Validate(invalidObj, out string errors));
            AssertError("The value '1' at 'root.longProp' should be less than or equal to 0.", errors);
        },
        () =>
        {
            using var invalidObj = ReadObject(new DynamicJsonValue
            {
                [longProp] = 0.5
            });

            Assert.False(schemaValidator.Validate(invalidObj, out string errors));
            AssertError("The value '0.5' at 'root.longProp' should be less than or equal to 0.", errors);
        },
        () =>
        {
            using var validObj = ReadObject(new DynamicJsonValue
            {
                [doubleProp] = 0.5
            });

            if (schemaValidator.Validate(validObj, out string errors) == false)
                Assert.Fail(string.Join("\n", errors));
        },
        () =>
        {
            using var invalidObj = ReadObject(new DynamicJsonValue
            {
                [doubleProp] = 1
            });

            Assert.False(schemaValidator.Validate(invalidObj, out string errors));
            AssertError("The value '1' at 'root.doubleProp' should be less than or equal to 0.5.", errors);
        },
        () =>
        {
            using var invalidObj = ReadObject(new DynamicJsonValue
            {
                [doubleProp] = 1.5
            });

            Assert.False(schemaValidator.Validate(invalidObj, out string errors));
            AssertError("The value '1.5' at 'root.doubleProp' should be less than or equal to 0.5.", errors);
        });
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateExclusiveMaximum()
    {
        const string longProp = "longProp";
        const string doubleProp = "doubleProp";

        var schemaValidator = new SchemaValidator();

        using (var schemaDefinition = ReadObject(new DynamicJsonValue
        {
           ["type"] = "object",
           ["properties"] = new DynamicJsonValue
           {
               [longProp] = new DynamicJsonValue
               {
                   ["maximum"] = 0,
                   ["exclusiveMaximum"] = true
               },
               [doubleProp] = new DynamicJsonValue
               {
                   ["maximum"] = 0.5,
                   ["exclusiveMaximum"] = true
               }
           }
        }))
        {
            schemaValidator.Init(schemaDefinition);
        }
        
        
        Assert.Multiple(() =>
        {
            var validObj = ReadObject(new DynamicJsonValue
            {
                [longProp] = -1
            });

            if (schemaValidator.Validate(validObj, out string errors) == false)
                Assert.Fail(string.Join("\n", errors));
        },
        () =>
        {
            var invalidObj = ReadObject(new DynamicJsonValue
            {
                [longProp] = 0
            });

            Assert.False(schemaValidator.Validate(invalidObj, out string errors));
            AssertError("The value '0' at 'root.longProp' should be less than 0.", errors);
        },
        () =>
        {
            using var invalidObj = ReadObject(new DynamicJsonValue
            {
                [longProp] = 0.5
            });

            Assert.False(schemaValidator.Validate(invalidObj, out string errors));
            AssertError("The value '0.5' at 'root.longProp' should be less than 0.", errors);
        },
        () =>
        {
            using var validObj = ReadObject(new DynamicJsonValue
            {
                [doubleProp] = -0.5
            });

            if (schemaValidator.Validate(validObj, out string errors) == false)
                Assert.Fail(string.Join("\n", errors));
        },
        () =>
        {
            using var invalidObj = ReadObject(new DynamicJsonValue
            {
                [doubleProp] = 1
            });

            Assert.False(schemaValidator.Validate(invalidObj, out string errors));
            AssertError("The value '1' at 'root.doubleProp' should be less than 0.5.", errors);
        },
        () =>
        {
            using var invalidObj = ReadObject(new DynamicJsonValue
            {
                [doubleProp] = 0.5
            });

            Assert.False(schemaValidator.Validate(invalidObj, out string errors));
            AssertError("The value '0.5' at 'root.doubleProp' should be less than 0.5.", errors);
        });
    }
    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateMultipleOf()
    {
        const string longProp = "longProp";
        const string doubleProp = "doubleProp";

        var schemaValidator = new SchemaValidator();

        using (var schemaDefinition = ReadObject(new DynamicJsonValue
        {
           ["type"] = "object",
           ["properties"] = new DynamicJsonValue
           {
               [longProp] = new DynamicJsonValue
               {
                   ["multipleOf"] = 3
               },
               [doubleProp] = new DynamicJsonValue
               {
                   ["multipleOf"] = 0.6
               }
           }
        }))
        {
            schemaValidator.Init(schemaDefinition);
        }
        
        
        Assert.Multiple(() =>
        {
            var validObj = ReadObject(new DynamicJsonValue
            {
                [longProp] = 3
            });

            if (schemaValidator.Validate(validObj, out string errors) == false)
                Assert.Fail(string.Join("\n", errors));
        }, 
        () =>
        {
            var validObj = ReadObject(new DynamicJsonValue
            {
                [longProp] = 6
            });

            if (schemaValidator.Validate(validObj, out string errors) == false)
                Assert.Fail(string.Join("\n", errors));
        },
        () =>
        {
            var validObj = ReadObject(new DynamicJsonValue
            {
                [longProp] = -9
            });

            if (schemaValidator.Validate(validObj, out string errors) == false)
                Assert.Fail(string.Join("\n", errors));
        },
        () =>
        {
            var invalidObj = ReadObject(new DynamicJsonValue
            {
                [longProp] = 4
            });

            Assert.False(schemaValidator.Validate(invalidObj, out string errors));
            AssertError("The value '4' at 'root.longProp' should be a multiple of 3.", errors);
        },
        () =>
        {
            var invalidObj = ReadObject(new DynamicJsonValue
            {
                [longProp] = 4.5
            });

            Assert.False(schemaValidator.Validate(invalidObj, out string errors));
            AssertError("The value '4.5' at 'root.longProp' should be a multiple of 3.", errors);
        },
        () =>
        {
            using var validObj = ReadObject(new DynamicJsonValue
            {
                [doubleProp] = 0.6
            });

            if (schemaValidator.Validate(validObj, out string errors) == false)
                Assert.Fail(string.Join("\n", errors));
        },
        () =>
        {
            using var validObj = ReadObject(new DynamicJsonValue
            {
                [doubleProp] = 6
            });

            if (schemaValidator.Validate(validObj, out string errors) == false)
                Assert.Fail(string.Join("\n", errors));
        },
        () =>
        {
            using var validObj = ReadObject(new DynamicJsonValue
            {
                [doubleProp] = -1.8
            });

            if (schemaValidator.Validate(validObj, out string errors) == false)
                Assert.Fail(string.Join("\n", errors));
        },
        () =>
        {
            using var invalidObj = ReadObject(new DynamicJsonValue
            {
                [doubleProp] = 0.5
            });

            Assert.False(schemaValidator.Validate(invalidObj, out string errors));
            AssertError("The value '0.5' at 'root.doubleProp' should be a multiple of 0.6.", errors);
        },
        () =>
        {
            using var invalidObj = ReadObject(new DynamicJsonValue
            {
                [doubleProp] = 1
            });

            Assert.False(schemaValidator.Validate(invalidObj, out string errors));
            AssertError("The value '1' at 'root.doubleProp' should be a multiple of 0.6.", errors);
        });
    }
}
