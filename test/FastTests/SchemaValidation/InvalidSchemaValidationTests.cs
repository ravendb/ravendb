using System.Collections.Generic;
using System.Linq;
using Raven.Server.Documents.SchemaValidation;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using SVC = Raven.Server.Documents.SchemaValidation.SchemaValidatorConstants;

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
            new object[] { SVC.Pattern, 78, "The value of 'pattern' must be a string, but received '78' of type 'integer'. Schema path '#/properties/prop/pattern'." },
            new object[] { SVC.MinLength, "somestring", "The value of 'minLength' must be an integer, but received 'somestring' of type 'string'. Schema path '#/properties/prop/minLength'." },
            new object[] { SVC.MaxLength, "somestring", "The value of 'maxLength' must be an integer, but received 'somestring' of type 'string'. Schema path '#/properties/prop/maxLength'." },
            new object[] { SVC.Maximum, "somestring", "The value of 'maximum' must be a number or an integer, but received 'somestring' of type 'string'. Schema path '#/properties/prop/maximum'." },
            new object[] { SVC.Minimum, "somestring", "The value of 'minimum' must be a number or an integer, but received 'somestring' of type 'string'. Schema path '#/properties/prop/minimum'." },
            new object[] { SVC.MultipleOf, "somestring", "The value of 'multipleOf' must be a number or an integer, but received 'somestring' of type 'string'. Schema path '#/properties/prop/multipleOf'." },
            new object[] { SVC.Enum, "somestring", "The value of 'enum' must be an array, but received 'somestring' of type 'string'. Schema path '#/properties/prop/enum'." },
            new object[] { SVC.Required, "somestring", "The value of 'required' must be an array, but received 'somestring' of type 'string'. Schema path '#/properties/prop/required'." },
            new object[] { SVC.Required, new DynamicJsonArray { 1 }, "The value of '[0]' must be a string, but received '1' of type 'integer'. Schema path '#/properties/prop[0]'." },
            new object[] { SVC.MinProperties, "somestring", "The value of 'minProperties' must be an integer, but received 'somestring' of type 'string'. Schema path '#/properties/prop/minProperties'." },
            new object[] { SVC.MaxProperties, "somestring", "The value of 'maxProperties' must be an integer, but received 'somestring' of type 'string'. Schema path '#/properties/prop/maxProperties'." },
            new object[] { SVC.PropertyNames, 1, "The value of 'propertyNames' must be an object, but received '1' of type 'integer'. Schema path '#/properties/prop/propertyNames'." },
            new object[] { SVC.UniqueItems, 1, "The value of 'uniqueItems' must be a boolean, but received '1' of type 'integer'. Schema path '#/properties/prop/uniqueItems'." },
            new object[] { SVC.PrefixItems, 1, "The value of 'prefixItems' must be an array, but received '1' of type 'integer'. Schema path '#/properties/prop/prefixItems'." },
            new object[] { SVC.Contains, 1, "The value of 'contains' must be an object, but received '1' of type 'integer'. Schema path '#/properties/prop/contains'." },
            new object[] { SVC.DependentRequired, 1, "The value of 'dependentRequired' must be an object, but received '1' of type 'integer'. Schema path '#/properties/prop/dependentRequired'." },
            new object[] { SVC.DependentRequired, new DynamicJsonValue { ["prop1"] = new DynamicJsonArray { 1 } }, "The value of '[0]' must be a string, but received '1' of type 'integer'. Schema path '#/properties/prop/dependentRequired[0]'." },
            new object[] { SVC.If, 1, "The value of 'if' must be an object, but received '1' of type 'integer'. Schema path '#/properties/prop/if'." },
            new object[] { SVC.DependentSchemas, 1, "The value of 'dependentSchemas' must be an object, but received '1' of type 'integer'. Schema path '#/properties/prop/dependentSchemas'." },
            new object[] { SVC.Not, 1, "The value of 'not' must be an object, but received '1' of type 'integer'. Schema path '#/properties/prop/not'." },
            new object[] { SVC.Ref, 1, "The value of '$ref' must be a string, but received '1' of type 'integer'. Schema path '#/properties/prop/$ref'." },
            new object[] { SVC.AllOf, 1, "The value of 'allOf' must be an array, but received '1' of type 'integer'. Schema path '#/properties/prop/allOf'." },
            new object[] { SVC.OneOf, 1, "The value of 'oneOf' must be an array, but received '1' of type 'integer'. Schema path '#/properties/prop/oneOf'." },
            new object[] { SVC.AnyOf, 1, "The value of 'anyOf' must be an array, but received '1' of type 'integer'. Schema path '#/properties/prop/anyOf'." },
        };

    [RavenFact(RavenTestCategory.JavaScript)]
    public void InvalidSchema_HasTestForAllRules()
    {
        var validWithAllValues = new []{"const"};
        var enumerable = TestCases.Select(x => x.First()).Concat(validWithAllValues);
        Assert.Equivalent(SchemaRuleValidatorFactoryHelper.TestingStuff.GetRuleNames(), enumerable);
    }    
    
    [RavenTheory(RavenTestCategory.JavaScript)]
    [MemberData(nameof(TestCases))]
    public void InvalidSchema_WhenDefineRuleWithWrongValue(string rule, object ruleValue, string error)
    {
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Properties] = new DynamicJsonValue
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
    [InlineData(SVC.Properties, "invalidvalue", "The value of 'properties' must be an object, but received 'invalidvalue' of type 'string'. Schema path '#/properties'.")]
    [InlineData(SVC.PatternProperties, "invalidvalue", "The value of 'patternProperties' must be an object, but received 'invalidvalue' of type 'string'. Schema path '#/patternProperties'.")]
    [InlineData(SVC.AdditionalProperties, "invalidvalue", "The value of 'additionalProperties' must be a boolean or an object, but received 'invalidvalue' of type 'string'. Schema path '#/additionalProperties'.")]
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
        var schemaValidator = new SchemaValidator();

        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        {
            var exception = Assert.Throws<InvalidSchemaValidationDefinitionException>(() => schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings));
            AssertError(error, exception.Message);
        }
    }
    
    [RavenTheory(RavenTestCategory.JavaScript)]
    [InlineData(SVC.Then, "The value of 'then' must be an object, but received '1' of type 'integer'. Schema path '#/then'.")]
    [InlineData(SVC.Else, "The value of 'else' must be an object, but received '1' of type 'integer'. Schema path '#/else'.")]

    public void InvalidSchema_WhenDefineConditionalSchemaWithWrongValue(string rule, string error)
    {
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.If] = new DynamicJsonValue(),
            [rule] = 1,
        };
        AssertInvalidSchemaThrow(schemaDefinition, error);
    }
}
