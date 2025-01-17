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
            new object[] { SVC.pattern, 78, "The value of 'pattern' at 'prop' must be a string, but received '78' of type 'integer'." },
            new object[] { SVC.minLength, "somestring", "The value of 'minLength' at 'prop' must be an integer, but received 'somestring' of type 'string'." },
            new object[] { SVC.maxLength, "somestring", "The value of 'maxLength' at 'prop' must be an integer, but received 'somestring' of type 'string'." },
            new object[] { SVC.maximum, "somestring", "The value of 'maximum' at 'prop' must be a number or an integer but received 'somestring' of type 'string'." },
            new object[] { SVC.minimum, "somestring", "The value of 'minimum' at 'prop' must be a number or an integer but received 'somestring' of type 'string'." },
            new object[] { SVC.multipleOf, "somestring", "The value of 'multipleOf' at 'prop' must be a number or an integer but received 'somestring' of type 'string'." },
            new object[] { SVC.@enum, "somestring", "The value of 'enum' at 'prop' must be an array, but received 'somestring' of type 'string'." },
            new object[] { SVC.required, "somestring", "The value of 'required' at 'prop' must be an array, but received 'somestring' of type 'string'." },
            new object[] { SVC.minProperties, "somestring", "The value of 'minProperties' at 'prop' must be an integer, but received 'somestring' of type 'string'." },
            new object[] { SVC.maxProperties, "somestring", "The value of 'maxProperties' at 'prop' must be an integer, but received 'somestring' of type 'string'." },
            new object[] { SVC.propertyNames, 1, "The value of 'propertyNames' at 'prop' must be an object, but received '1' of type 'integer'." },
            new object[] { SVC.uniqueItems, 1, "The value of 'uniqueItems' at 'prop' must be a boolean, but received '1' of type 'integer'." },
            new object[] { SVC.prefixItems, 1, "The value of 'prefixItems' at 'prop' must be an array, but received '1' of type 'integer'." },
            new object[] { SVC.contains, 1, "The value of 'contains' at 'prop' must be an object, but received '1' of type 'integer'." },
            new object[] { SVC.dependentRequired, 1, "The value of 'dependentRequired' at 'prop' must be an object, but received '1' of type 'integer'." },
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
        var schemaValidator = new SchemaValidator(ContextPool);

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
        using (ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition))
        {
            var exception = Assert.Throws<InvalidSchemaValidationDefinitionException>(() => schemaValidator.Init(blitSchemaDefinition));
            AssertError(error, exception.Message);
        }
    }
    
    [RavenTheory(RavenTestCategory.JavaScript)]
    [InlineData(SVC.properties, "invalidvalue", "The value of 'properties' at '' must be an object, but received a value of type 'string'.")]
    [InlineData(SVC.patternProperties, "invalidvalue", "The value of 'patternProperties' at '' must be an object, but received a value of type 'string'.")]
    [InlineData(SVC.additionalProperties, "invalidvalue", "The value of 'additionalProperties' at '' must be a boolean or an object, but received a value of type 'string'.")]
    public void InvalidSchema_WhenDefineWithWrongValue(string key, object value, string error)
    {
        var schemaValidator = new SchemaValidator(ContextPool);

        var schemaDefinition = new DynamicJsonValue
        {
            [key] = value
        };

        using (ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition))
        {
            var exception = Assert.Throws<InvalidSchemaValidationDefinitionException>(() => schemaValidator.Init(blitSchemaDefinition));
            AssertError(error, exception.Message);
        }
    }
}
