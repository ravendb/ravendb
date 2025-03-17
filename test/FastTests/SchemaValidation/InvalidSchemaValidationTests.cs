using System.Collections.Generic;
using System.Linq;
using Raven.Server.SchemaValidation;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using SVC = Raven.Server.SchemaValidation.SchemaValidatorConstants;

namespace FastTests.SchemaValidation;

public class InvalidSchemaValidationTests : SchemaValidationTestsBase
{
    // ReSharper disable once ConvertToPrimaryConstructor
    public InvalidSchemaValidationTests(ITestOutputHelper output) : base(output)
    {
    }
    
    public static IEnumerable<object[]> TestCases =>
        new List<object[]>
        {
            new object[] { SVC.pattern, 78, "The value of 'pattern' must be a string, but received '78' of type 'integer'. Schema path 'properties.prop'." },
            new object[] { SVC.minLength, "somestring", "The value of 'minLength' must be an integer, but received 'somestring' of type 'string'. Schema path 'properties.prop'." },
            new object[] { SVC.maxLength, "somestring", "The value of 'maxLength' must be an integer, but received 'somestring' of type 'string'. Schema path 'properties.prop'." },
            new object[] { SVC.maximum, "somestring", "The value of 'maximum' must be a number or an integer but received 'somestring' of type 'string'. Schema path 'properties.prop'." },
            new object[] { SVC.minimum, "somestring", "The value of 'minimum' must be a number or an integer but received 'somestring' of type 'string'. Schema path 'properties.prop'." },
            new object[] { SVC.multipleOf, "somestring", "The value of 'multipleOf' must be a number or an integer but received 'somestring' of type 'string'. Schema path 'properties.prop'." },
            new object[] { SVC.@enum, "somestring", "The value of 'enum' must be an array, but received 'somestring' of type 'string'. Schema path 'properties.prop'." },
            new object[] { SVC.required, "somestring", "The value of 'required' must be an array, but received 'somestring' of type 'string'. Schema path 'properties.prop'." },
            new object[] { SVC.minProperties, "somestring", "The value of 'minProperties' must be an integer, but received 'somestring' of type 'string'. Schema path 'properties.prop'." },
            new object[] { SVC.maxProperties, "somestring", "The value of 'maxProperties' must be an integer, but received 'somestring' of type 'string'. Schema path 'properties.prop'." },
            new object[] { SVC.propertyNames, 1, "The value of 'propertyNames' must be an object, but received '1' of type 'integer'. Schema path 'properties.prop'." },
            new object[] { SVC.uniqueItems, 1, "The value of 'uniqueItems' must be a boolean, but received '1' of type 'integer'. Schema path 'properties.prop'." },
            new object[] { SVC.prefixItems, 1, "The value of 'prefixItems' must be an array, but received '1' of type 'integer'. Schema path 'properties.prop'." },
            new object[] { SVC.contains, 1, "The value of 'contains' must be an object, but received '1' of type 'integer'. Schema path 'properties.prop'." },
            new object[] { SVC.dependentRequired, 1, "The value of 'dependentRequired' must be an object, but received '1' of type 'integer'. Schema path 'properties.prop'." },
            new object[] { SVC.@if, 1, "The value of 'if' must be an object, but received '1' of type 'integer'. Schema path 'properties.prop'." },
            new object[] { SVC.dependentSchemas, 1, "The value of 'dependentSchemas' must be an object, but received '1' of type 'integer'. Schema path 'properties.prop'." },
            new object[] { SVC.not, 1, "The value of 'not' must be an object, but received '1' of type 'integer'. Schema path 'properties.prop'." },
        };

    [RavenFact(RavenTestCategory.JavaScript)]
    public void InvalidSchema_HasTestForAllRules()
    {
        var validWithAllValues = new []{"const"};
        var enumerable = TestCases.Select(x => x.First()).Concat(validWithAllValues);
        Assert.Equivalent(SchemaRuleValidatorFactoryHelper.ForTestGetRuleNames(), enumerable);
    }    
    
    [RavenTheory(RavenTestCategory.JavaScript)]
    [MemberData(nameof(TestCases))]
    public void InvalidSchema_WhenDefineRuleWithWrongValue(string rule, object ruleValue, string error)
    {
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.properties] = new DynamicJsonValue
            {
                ["prop"] = new DynamicJsonValue
                {
                    [rule] = ruleValue
                }
            }
        };
        AssertInvalidSchemaThrow(schemaDefinition, error);
    }
    
    [RavenTheory(RavenTestCategory.JavaScript)]
    [InlineData(SVC.properties, "invalidvalue", "The value of 'properties' must be an 'object', but received a value of type 'string'. Schema path 'properties'.")]
    [InlineData(SVC.patternProperties, "invalidvalue", "The value of 'patternProperties' must be an 'object', but received a value of type 'string'. Schema path 'patternProperties'.")]
    [InlineData(SVC.additionalProperties, "invalidvalue", "The value of 'additionalProperties' must be a 'boolean' or an 'object', but received a value of type 'string'. Schema path 'additionalProperties'.")]
    public void InvalidSchema_WhenDefineWithWrongValue(string key, object value, string error)
    {
        var schemaDefinition = new DynamicJsonValue
        {
            [key] = value
        };
        AssertInvalidSchemaThrow(schemaDefinition, error);
    }

    private void AssertInvalidSchemaThrow(DynamicJsonValue schemaDefinition, string error)
    {
        var schemaValidator = new SchemaValidator(ContextPool);

        using (ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition))
        {
            var exception = Assert.Throws<InvalidSchemaValidationDefinitionException>(() => schemaValidator.Init(blitSchemaDefinition));
            AssertError(error, exception.Message);
        }
    }
    
    [RavenTheory(RavenTestCategory.JavaScript)]
    [InlineData(SVC.then, "The value of 'then' must be an object, but received '1' of type 'integer'. Schema path ''.")]
    [InlineData(SVC.@else, "The value of 'else' must be an object, but received '1' of type 'integer'. Schema path ''.")]

    public void InvalidSchema_WhenDefineConditionalSchemaWithWrongValue(string rule, string error)
    {
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.@if] = new DynamicJsonValue{},
            [rule] = 1,
        };
        AssertInvalidSchemaThrow(schemaDefinition, error);
    }
}
