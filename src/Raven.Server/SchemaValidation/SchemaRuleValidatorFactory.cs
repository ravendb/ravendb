using System;
using System.Linq;
using System.Reflection;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation;

public interface ISchemaRuleValidatorFactory
{
    ISchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath);
}

public abstract class SchemaRuleValidatorFactory<T> : ISchemaRuleValidatorFactory where T : ISchemaRuleValidator
{
    protected readonly string Rule = typeof(T).GetCustomAttribute<SchemaRuleAttribute>()?.Rule 
                                     ?? throw new InvalidOperationException($"The type '{typeof(T).Name}' must have a SchemaRuleAttribute defined with a Rule property.");

    protected readonly BlittableJsonToken[] NumberTypes = [BlittableJsonToken.Integer, BlittableJsonToken.LazyNumber];

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

    protected static string GetStringOrThrow(string rule, BlittableJsonReaderObject schemaDefinition, string schemaPath, BlittableJsonToken type)
    {
        if (type != BlittableJsonToken.String)
            TrowRuleTypeError(rule, schemaDefinition[rule], BlittableJsonToken.String, type, schemaPath);

        if (schemaDefinition.TryGet(rule, out string pattern) == false)
            throw new InvalidOperationException($"'{rule}' must to be convertable to {nameof(System.String)} here. Should not happen");
        return pattern;
    }
    
    protected static BlittableJsonReaderObject GetObjOrThrow(string rule, BlittableJsonReaderObject schemaDefinition, string schemaPath, BlittableJsonToken type)
    {
        if (type != BlittableJsonToken.StartObject)
            TrowRuleTypeError(rule, schemaDefinition[rule], BlittableJsonToken.StartObject, type, schemaPath);

        if (schemaDefinition.TryGet(rule, out BlittableJsonReaderObject pattern) == false)
            throw new InvalidOperationException($"'{rule}' must to be convertable to {nameof(BlittableJsonReaderObject)} here. Should not happen");
        return pattern;
    }
}
