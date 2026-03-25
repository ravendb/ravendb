using System;
using System.Threading.Tasks;
using Raven.Server.Documents.SchemaValidation;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.SchemaValidation;

public class RefDefSchemaValidationTests : SchemaValidationTestsBase
{
    // ReSharper disable once ConvertToPrimaryConstructor
    public RefDefSchemaValidationTests(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenRestrictOnStringWithReferenced()
    {
        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            [SchemaValidatorConstants.Properties] = new DynamicJsonValue
            {
                ["prop1"] = new DynamicJsonValue
                {
                    [SchemaValidatorConstants.Ref] = "#/$defs/mySchema"
                },
                ["prop2"] = new DynamicJsonValue
                {
                    [SchemaValidatorConstants.Ref] = "#/$defs/mySchema"
                },
            },
            [SchemaValidatorConstants.Defs] = new DynamicJsonValue
            {
                ["mySchema"] = new DynamicJsonValue
                {
                    [SchemaValidatorConstants.MaxLength] = 5
                }
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["prop1"] = "1234", 
                    ["prop2"] = "123",
                }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["prop3"] = "value1"
                }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["prop1"] = "123456", 
                    ["prop2"] = "1234567"
                }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError($"The length of the value '123456' at 'prop1' should not exceed 5, but its actual length is 6.{Environment.NewLine}The length of the value '1234567' at 'prop2' should not exceed 5, but its actual length is 7.", errors);
            });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenRestrictOnNumberWithReferenced()
    {
        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            [SchemaValidatorConstants.Properties] = new DynamicJsonValue
            {
                ["prop1"] = new DynamicJsonValue
                {
                    [SchemaValidatorConstants.Ref] = "#/$defs/mySchema"
                },
                ["prop2"] = new DynamicJsonValue
                {
                    [SchemaValidatorConstants.Ref] = "#/$defs/mySchema"
                },
            },
            [SchemaValidatorConstants.Defs] = new DynamicJsonValue
            {
                ["mySchema"] = new DynamicJsonValue
                {
                    [SchemaValidatorConstants.Maximum] = 5
                }
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["prop1"] = 4, 
                    ["prop2"] = 3.4,
                }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["prop1"] = 6, 
                    ["prop2"] = 7.3
                }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError($"The value '6' at 'prop1' should be less than or equal to 5.{Environment.NewLine}The value '7.3' at 'prop2' should be less than or equal to 5.", errors);
            });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenRestrictOnBooleanWithReferenced()
    {
        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            [SchemaValidatorConstants.Properties] = new DynamicJsonValue
            {
                ["prop1"] = new DynamicJsonValue
                {
                    [SchemaValidatorConstants.Ref] = "#/$defs/mySchema"
                },
                ["prop2"] = new DynamicJsonValue
                {
                    [SchemaValidatorConstants.Ref] = "#/$defs/mySchema"
                },
            },
            [SchemaValidatorConstants.Defs] = new DynamicJsonValue
            {
                ["mySchema"] = new DynamicJsonValue
                {
                    [SchemaValidatorConstants.Const] = true
                }
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["prop1"] = true,
                    ["prop2"] = true
                }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["prop1"] = false, 
                    ["prop2"] = false
                }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError($"The value at 'prop1' must be 'True', but it is 'False'.{Environment.NewLine}The value at 'prop2' must be 'True', but it is 'False'", errors);
            });
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenRestrictOnNullWithReferenced()
    {
        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            [SchemaValidatorConstants.Properties] = new DynamicJsonValue
            {
                ["prop1"] = new DynamicJsonValue
                {
                    [SchemaValidatorConstants.Ref] = "#/$defs/mySchema"
                },
                ["prop2"] = new DynamicJsonValue
                {
                    [SchemaValidatorConstants.Ref] = "#/$defs/mySchema"
                },
            },
            [SchemaValidatorConstants.Defs] = new DynamicJsonValue
            {
                ["mySchema"] = new DynamicJsonValue
                {
                    [SchemaValidatorConstants.Const] = null
                }
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["prop1"] = null,
                    ["prop2"] = null
                }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["prop1"] = "somethingelse", 
                    ["prop2"] = "somethingelse"
                }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError($"The value at 'prop1' must be '', but it is '\"somethingelse\"'.{Environment.NewLine}The value at 'prop2' must be '', but it is '\"somethingelse\"'.", errors);
            });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenRestrictOnObjectWithReferenced()
    {
        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            [SchemaValidatorConstants.Properties] = new DynamicJsonValue
            {
                ["prop1"] = new DynamicJsonValue
                {
                    [SchemaValidatorConstants.Ref] = "#/$defs/mySchema"
                }
            },
            [SchemaValidatorConstants.Defs] = new DynamicJsonValue
            {
                ["mySchema"] = new DynamicJsonValue
                {
                    [SchemaValidatorConstants.MinProperties] = 1
                }
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["prop1"] = new DynamicJsonValue
                    {
                        ["nestedProp"] = "somevalue"
                    }
                }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["prop1"] = new DynamicJsonValue()
                }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError($"The object at 'prop1' must have at least 1 property, but it has only 0.", errors);
            });
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenRestrictOnArrayWithReferenced()
    {
        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            [SchemaValidatorConstants.Properties] = new DynamicJsonValue
            {
                ["prop1"] = new DynamicJsonValue
                {
                    [SchemaValidatorConstants.Ref] = "#/$defs/mySchema"
                }
            },
            [SchemaValidatorConstants.Defs] = new DynamicJsonValue
            {
                ["mySchema"] = new DynamicJsonValue
                {
                    [SchemaValidatorConstants.Contains] = new DynamicJsonValue { ["type"]= "number"}
                }
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["prop1"] = new DynamicJsonArray{1}
                }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["prop1"] = new string []{}
                }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("""The array at 'prop1' must contain at least 1 items matching the required schema, but no items where found. Schema : {"type":"number"}""", errors);
            });
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public void RefSchema_WhenHasCircularReferences_ShouldFail()
    {
        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            [SchemaValidatorConstants.Defs] = new DynamicJsonValue
            {
                ["firstSchema"] = new DynamicJsonValue
                {
                    [SchemaValidatorConstants.Properties] = new DynamicJsonValue
                    {
                        ["prop1"] = new DynamicJsonValue
                        {
                            [SchemaValidatorConstants.Ref] = "#/$defs/secondSchema"
                        },
                    }
                },
                ["secondSchema"] = new DynamicJsonValue
                {
                    [SchemaValidatorConstants.Properties] = new DynamicJsonValue
                    {
                        ["prop1"] = new DynamicJsonValue
                        {
                            [SchemaValidatorConstants.Ref] = "#/$defs/firstSchema"
                        },
                    }
                }
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        {
            var exception = Assert.ThrowsAny<InvalidSchemaValidationDefinitionException>(() => schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings));
            AssertError("A circular reference was detected at '#/$defs/firstSchema/properties/prop1/$ref/properties/prop1/$ref/properties/prop1/$ref'.", exception.Message);
        }
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenRestrictOnAllOf()
    {
        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            [SchemaValidatorConstants.Properties] = new DynamicJsonValue
            {
                ["myprop"] = new DynamicJsonValue
                {
                    [SchemaValidatorConstants.AllOf] = new DynamicJsonArray
                    {
                        new DynamicJsonValue
                        {
                            [SchemaValidatorConstants.Ref] = "#/$defs/mySchema1"
                        },
                        new DynamicJsonValue
                        {
                            [SchemaValidatorConstants.Ref] = "#/$defs/mySchema2"
                        }
                    }
                }
            },
            [SchemaValidatorConstants.Defs] = new DynamicJsonValue
            {
                ["mySchema1"] = new DynamicJsonValue
                {
                    [SchemaValidatorConstants.MaxLength] = 5
                },
                ["mySchema2"] = new DynamicJsonValue
                {
                    [SchemaValidatorConstants.Pattern] = "^\\d"
                }
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["myprop"] = "1234", 
                }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["myprop"] = "a12345"
                }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError($"The length of the value 'a12345' at 'myprop' should not exceed 5, but its actual length is 6.{Environment.NewLine}The pattern of the value 'a12345' at 'myprop' does not match the required pattern '^\\d'", errors);
            });
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenRestrictOnOneOf()
    {
        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            [SchemaValidatorConstants.Properties] = new DynamicJsonValue
            {
                ["myprop"] = new DynamicJsonValue
                {
                    [SchemaValidatorConstants.OneOf] = new DynamicJsonArray
                    {
                        new DynamicJsonValue
                        {
                            [SchemaValidatorConstants.Ref] = "#/$defs/mySchema1"
                        },
                        new DynamicJsonValue
                        {
                            [SchemaValidatorConstants.Ref] = "#/$defs/mySchema2"
                        }
                    }
                }
            },
            [SchemaValidatorConstants.Defs] = new DynamicJsonValue
            {
                ["mySchema1"] = new DynamicJsonValue
                {
                    [SchemaValidatorConstants.MaxLength] = 5
                },
                ["mySchema2"] = new DynamicJsonValue
                {
                    [SchemaValidatorConstants.Pattern] = "^\\d"
                }
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["myprop"] = "123456", 
                }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["myprop"] = "a12", 
                }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["myprop"] = "1234"
                }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The value at 'myprop' matches more than one schema, but it must match exactly one.", errors);
            });
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenRestrictAnyOneOf()
    {
        var schemaValidator = new SchemaValidator();
        var schemaDefinition = new DynamicJsonValue
        {
            [SchemaValidatorConstants.Properties] = new DynamicJsonValue
            {
                ["myprop"] = new DynamicJsonValue
                {
                    [SchemaValidatorConstants.AnyOf] = new DynamicJsonArray
                    {
                        new DynamicJsonValue
                        {
                            [SchemaValidatorConstants.Ref] = "#/$defs/mySchema1"
                        },
                        new DynamicJsonValue
                        {
                            [SchemaValidatorConstants.Ref] = "#/$defs/mySchema2"
                        },
                        new DynamicJsonValue
                        {
                            [SchemaValidatorConstants.Ref] = "#/$defs/mySchema3"
                        }
                    }
                }
            },
            [SchemaValidatorConstants.Defs] = new DynamicJsonValue
            {
                ["mySchema1"] = new DynamicJsonValue
                {
                    [SchemaValidatorConstants.Minimum] = 5
                },
                ["mySchema2"] = new DynamicJsonValue
                {
                    [SchemaValidatorConstants.MultipleOf] = 2
                },
                ["mySchema3"] = new DynamicJsonValue
                {
                    [SchemaValidatorConstants.Const] = 1
                }
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        await AssertMultipleParallel(() =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["myprop"] = 1 
                }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["myprop"] = 4, 
                }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["myprop"] = 7, 
                }, out var obj);

                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            },
            () =>
            {
                using var ctx = ReadObjectOnNewCtx(new DynamicJsonValue
                {
                    ["myprop"] = 3
                }, out var obj);

                Assert.False(schemaValidator.Validate(obj, out var errors));
                AssertError("The value at 'myprop' does not match any of the schema restrictions.", errors);
            });
    }
}
