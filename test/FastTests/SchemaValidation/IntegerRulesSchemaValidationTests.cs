using Raven.Server.SchemaValidation;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.SchemaValidation;

public class IntegerRulesSchemaValidationTests
{
    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateMinimum()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();
        var schemaDefinition = context.ReadObject(new DynamicJsonValue
        {
            ["type"] = "object",
            ["properties"] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue
                {
                    ["type"] = "integer",
                    ["minimum"] = 0
                }
            }
        }, "schema Definition");
        
        var validObj = context.ReadObject(new DynamicJsonValue
        {
            ["prop"] = 0
        }, "test object");

        var invalidObj = context.ReadObject(new DynamicJsonValue
        {
            ["prop"] = -1
        }, "test object");
        
        var schemaValidator = new SchemaValidator(schemaDefinition);
        
        if (schemaValidator.Validate(validObj, out string errors) == false)
            Assert.Fail(string.Join("\n", errors));
        
        Assert.False(schemaValidator.Validate(invalidObj, out string _));
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateExclusiveMinimum()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();
        var schemaDefinition = context.ReadObject(new DynamicJsonValue
        {
            ["type"] = "object",
            ["properties"] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue
                {
                    ["type"] = "integer",
                    ["minimum"] = 0,
                    ["exclusiveMinimum"] = true
                }
            }
        }, "schema Definition");
        
        var validObj = context.ReadObject(new DynamicJsonValue
        {
            ["prop"] = 1
        }, "test object");

        var invalidObj = context.ReadObject(new DynamicJsonValue
        {
            ["prop"] = 0
        }, "test object");
        
        var schemaValidator = new SchemaValidator(schemaDefinition);
        
        if (schemaValidator.Validate(validObj, out string errors) == false)
            Assert.Fail(string.Join("\n", errors));
        
        Assert.False(schemaValidator.Validate(invalidObj, out string _));
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateMaximum()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();
        var schemaDefinition = context.ReadObject(new DynamicJsonValue
        {
            ["type"] = "object",
            ["properties"] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue
                {
                    ["type"] = "integer",
                    ["maximum"] = 0
                }
            }
        }, "schema Definition");
        
        var validObj = context.ReadObject(new DynamicJsonValue
        {
            ["prop"] = 0
        }, "test object");

        var invalidObj = context.ReadObject(new DynamicJsonValue
        {
            ["prop"] = 1
        }, "test object");
        
        var schemaValidator = new SchemaValidator(schemaDefinition);
        
        if (schemaValidator.Validate(validObj, out string errors) == false)
            Assert.Fail(string.Join("\n", errors));
        
        Assert.False(schemaValidator.Validate(invalidObj, out string _));
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateExclusiveMaximum()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();
        var schemaDefinition = context.ReadObject(new DynamicJsonValue
        {
            ["type"] = "object",
            ["properties"] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue
                {
                    ["type"] = "integer",
                    ["maximum"] = 0,
                    ["exclusiveMaximum"] = true
                }
            }
        }, "schema Definition");
        
        var validObj = context.ReadObject(new DynamicJsonValue
        {
            ["prop"] = -1
        }, "test object");

        var invalidObj = context.ReadObject(new DynamicJsonValue
        {
            ["prop"] = 0
        }, "test object");
        
        var schemaValidator = new SchemaValidator(schemaDefinition);
        
        if (schemaValidator.Validate(validObj, out string errors) == false)
            Assert.Fail(string.Join("\n", errors));
        
        Assert.False(schemaValidator.Validate(invalidObj, out string _));
    }
}
