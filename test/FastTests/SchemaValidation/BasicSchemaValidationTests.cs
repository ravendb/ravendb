using System;
using System.Threading.Tasks;
using Raven.Server.SchemaValidation;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.SchemaValidation;

public class BasicSchemaValidationTests
{
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

        var schemaValidator = new SchemaValidator(schemaDefinition);
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

        var schemaValidator = new SchemaValidator(schemaDefinition);
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

        var schemaValidator = new SchemaValidator(schemaDefinition);
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

        var schemaValidator = new SchemaValidator(schemaDefinition);
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

        var schemaValidator = new SchemaValidator(schemaDefinition);
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

        var schemaValidator = new SchemaValidator(schemaDefinition);
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

        var schemaValidator = new SchemaValidator(schemaDefinition);
        Assert.False(schemaValidator.Validate(testObj, out string errors));
        //TODO Check exact error
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public void SchemaValidation_WhenValidateIntPropertyAndLong_ShouldFail()
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
            ["prop"] = long.MaxValue
        }, "test object");

        var schemaValidator = new SchemaValidator(schemaDefinition);
        Assert.False(schemaValidator.Validate(testObj, out string errors));
        //TODO Check exact error
    }
}
