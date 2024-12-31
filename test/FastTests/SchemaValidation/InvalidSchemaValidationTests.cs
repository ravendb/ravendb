using System.Collections.Generic;
using System.Linq;
using Raven.Server.SchemaValidation;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

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
            new object[] { "pattern", 78, "The value of 'pattern' at 'prop' must be a string, but received '78' of type 'integer'." },
            new object[] { "minLength", "somestring", "The value of 'minLength' at 'prop' must be an integer, but received 'somestring' of type 'string'." },
            new object[] { "maxLength", "somestring", "The value of 'maxLength' at 'prop' must be an integer, but received 'somestring' of type 'string'." },
            new object[] { "maximum", "somestring", "The value of 'maximum' at 'prop' must be integer, number, but received 'somestring' of type 'string'." },
            new object[] { "minimum", "somestring", "The value of 'minimum' at 'prop' must be integer, number, but received 'somestring' of type 'string'." },
            new object[] { "multipleOf", "somestring", "The value of 'multipleOf' at 'prop' must be integer, number, but received 'somestring' of type 'string'." },
            new object[] { "enum", "somestring", "The value of 'enum' at 'prop' must be an array, but received 'somestring' of type 'string'." },
            new object[] { "required", "somestring", "The value of 'required' at 'prop' must be an array, but received 'somestring' of type 'string'." },
            new object[] { "minProperties", "somestring", "The value of 'minProperties' at 'prop' must be an integer, but received 'somestring' of type 'string'." },
            new object[] { "maxProperties", "somestring", "The value of 'maxProperties' at 'prop' must be an integer, but received 'somestring' of type 'string'." },
        };

    [RavenFact(RavenTestCategory.JavaScript)]
    public void InvalidSchema_HasTestForAllRules()
    {
        var validWithAllValues = new []{"const"};
        var enumerable = TestCases.Select(x => x.First()).Concat(validWithAllValues);
        Assert.Equivalent(SchemaRuleValidatorFactory.ForTestGetRuleNames(), enumerable);
    }    
    
    [RavenTheory(RavenTestCategory.JavaScript)]
    [MemberData(nameof(TestCases))]
    public void InvalidSchema_WhenDefineRuleWithWrongValue(string rule, object ruleValue, string error)
    {
        var schemaValidator = new SchemaValidator();

        var schemaDefinition = new DynamicJsonValue
        {
            ["properties"] = new DynamicJsonValue
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
    [InlineData("properties", "invalidvalue", "The value of 'properties' at '' must be an object, but received a value of type 'string'.")]
    [InlineData("patternProperties", "invalidvalue", "The value of 'patternProperties' at '' must be an object, but received a value of type 'string'.")]
    [InlineData("additionalProperties", "invalidvalue", "The value of 'additionalProperties' at '' must be a boolean or an object, but received a value of type 'string'.")]
    public void InvalidSchema_WhenDefineWithWrongValue(string key, object value, string error)
    {
        var schemaValidator = new SchemaValidator();

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
