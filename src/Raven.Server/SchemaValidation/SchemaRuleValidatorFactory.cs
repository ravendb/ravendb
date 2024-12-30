using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.SchemaValidation.Number;
using Raven.Server.SchemaValidation.Object;
using Raven.Server.SchemaValidation.String;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation;

public interface ISchemaRuleValidatorFactory
{
    ISchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath);
}

public abstract class SchemaRuleValidatorFactory : ISchemaRuleValidatorFactory
{
    private static readonly Dictionary<string, ISchemaRuleValidatorFactory> SchemaRuleValidatorFactories = new Dictionary<string, ISchemaRuleValidatorFactory>
    {
        #region numbers
        {MaximumSchemaRuleValidator.RuleName, new MaximumSchemaRuleValidatorFactory()},
        {MinimumSchemaRuleValidator.RuleName, new MinimumSchemaRuleValidatorFactory()},
        {MultipleOfSchemaRuleValidator.RuleName, new MultipleOfSchemaRuleValidatorFactory()},
        #endregion
        
        //TODO To find better name
        #region objects
        {ConstantSchemaRuleValidator.RuleName, new ConstantSchemaRuleValidatorFactory()},
        {EnumSchemaRuleValidator.RuleName, new EnumSchemaRuleValidatorFactory()},
        {RequiredSchemaRuleValidator.RuleName, new RequiredSchemaRuleValidatorFactory()},
        #endregion
        
        #region strings
        {MaximumLengthSchemaRuleValidator.RuleName, new MaximumLengthSchemaRuleValidatorFactory()},
        {MinimumLengthSchemaRuleValidator.RuleName, new MinimumLengthSchemaRuleValidatorFactory()},
        {PatternSchemaRuleValidator.RuleName, new PatternSchemaRuleValidatorFactory()},
        #endregion
    };
    
    protected readonly BlittableJsonToken[] NumberTypes = [BlittableJsonToken.Integer, BlittableJsonToken.LazyNumber];

    public static bool TryCreateValidator(string rule, BlittableJsonReaderObject schemaDefinition, string schemaPath, out ISchemaRuleValidator validator)
    {
        if (SchemaRuleValidatorFactories.TryGetValue(rule, out ISchemaRuleValidatorFactory factory))
        {
            validator = factory.Create(schemaDefinition, schemaPath);
            return true;
        }
        validator = null;
        return false;
    }

    //TODO Maybe to remove
    protected abstract string Rule { get; }
    
    public abstract ISchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath);

    protected static void TrowRuleTypeError(string rule, object ruleValue, BlittableJsonToken expectedType, BlittableJsonToken actualType, string schemaPath)
    {
        var publicType = SchemaValidationHelper.GetPublicType(expectedType);
        throw new InvalidSchemaValidationDefinitionException(
            $"The value of '{rule}' at '{schemaPath}' must be {GetIndefiniteArticle(publicType)} {publicType}, but received '{ruleValue}' of type '{SchemaValidationHelper.GetPublicType(actualType)}'.");
    }

    private static string GetIndefiniteArticle(string word)
    {
        if (string.IsNullOrEmpty(word))
            throw new ArgumentException("Input word cannot be null or empty.");

        return "aeiouAEIOU".Contains(word[0]) ? "an" : "a";
    }
    
    protected static void TrowRuleTypeError(string rule, object ruleValue, BlittableJsonToken[] expectedTypes, BlittableJsonToken actualType, string schemaPath)
    {
        throw new InvalidSchemaValidationDefinitionException(
            $"The value of '{rule}' at '{schemaPath}' must be {string.Join(", ", expectedTypes.Select(SchemaValidationHelper.GetPublicType))}, but received '{ruleValue}' of type '{SchemaValidationHelper.GetPublicType(actualType)}'.");
    }

    protected static bool TryGetPropertyType(BlittableJsonReaderObject obj, string prop, out BlittableJsonToken type)
    {
        if(obj.TryGetPropertyType(prop, out type) == false)
            return false;

        type &= BlittableJsonReaderBase.TypesMask;
        return true;
    }

    internal static string[] ForTestGetRuleNames() => SchemaRuleValidatorFactories.Keys.ToArray();
}
