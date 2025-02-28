using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation;

public static class SchemaValidationHelper
{
    private const string Null = "null";
    private const string Integer = "integer";
    private const string Number = "number";
    private const string String = "string";
    private const string Boolean = "boolean";
    private const string Object = "object";
    private const string Array = "array";

    public static string[] PublicTypes { get; }
    private static Dictionary<IComparable<string>, BlittableJsonToken[]> StringTypeToBlittableToken { get; }

    static SchemaValidationHelper()
    {
        StringTypeToBlittableToken = new Dictionary<IComparable<string>, BlittableJsonToken[]>()
        {
            { Null, [BlittableJsonToken.Null] },
            { Integer, [BlittableJsonToken.Integer] },
            { Number, [BlittableJsonToken.Integer, BlittableJsonToken.LazyNumber] },
            { String, [BlittableJsonToken.String, BlittableJsonToken.CompressedString] },
            { Boolean, [BlittableJsonToken.Boolean] },
            { Object, [BlittableJsonToken.StartObject] },
            { Array, [BlittableJsonToken.StartArray] }
        };

        PublicTypes = StringTypeToBlittableToken.Select(x => x.Key.ToString()).ToArray();
    }
    
    public static bool TryGetTokensForType(IComparable<string> type, out BlittableJsonToken[] tokens)
    {
        if (type.CompareTo(Null) == 0)
        {
            tokens = [BlittableJsonToken.Null];
            return true;
        }
        if (type.CompareTo(Integer) == 0)
        {
            tokens = [BlittableJsonToken.Integer];
            return true;
        }
        if (type.CompareTo(Number) == 0)
        {
            tokens = [BlittableJsonToken.Integer, BlittableJsonToken.LazyNumber];
            return true;
        }
        if (type.CompareTo(String) == 0)
        {
            tokens = [BlittableJsonToken.String, BlittableJsonToken.String];
            return true;
        }
        if (type.CompareTo(Boolean) == 0)
        {
            tokens = [BlittableJsonToken.Boolean];
            return true;
        }
        if (type.CompareTo(Object) == 0)
        {
            tokens = [BlittableJsonToken.StartObject];
            return true;
        }
        if (type.CompareTo(Array) == 0)
        {
            tokens = [BlittableJsonToken.StartArray];
            return true;
        }

        tokens = null;
        return false;
    }

    public static string GetPublicTypeOfObj(object obj) => GetPublicType(obj?.GetType());

    private static string GetPublicType(Type type)
    {
        if (type == null)
            return Null;
        if (type == typeof(long))
            return Integer;
        if (type == typeof(LazyNumberValue))
            return Number;
        if (type == typeof(LazyStringValue) || type == typeof(LazyCompressedStringValue))
            return String;
        if (type == typeof(bool))
            return Boolean;
        if (type == typeof(BlittableJsonReaderObject))
            return Object;
        if (type == typeof(BlittableJsonReaderArray))
            return Array;

        throw new NotImplementedException($"The type '{type.FullName}' is not supported.");
    }

    public static string GetPublicType(BlittableJsonToken type)
    {
        return type switch
        {
            BlittableJsonToken.Null => Null,
            BlittableJsonToken.Integer => Integer,
            BlittableJsonToken.LazyNumber => Number,
            BlittableJsonToken.String or BlittableJsonToken.CompressedString => String,
            BlittableJsonToken.Boolean => Boolean,
            BlittableJsonToken.StartObject => Object,
            BlittableJsonToken.StartArray => Array,
            _ => throw new NotImplementedException($"The type '{type}' is not supported.")
        };
    }
   

    private static string GetIndefiniteArticle(string word)
    {
        if (string.IsNullOrEmpty(word))
            throw new ArgumentException("Input word cannot be null or empty.");

        return "aeiouAEIOU".Contains(word[0]) ? "an" : "a";
    }
    
    public static bool TryGetBoolean(BlittableJsonReaderObject schemaDefinition, string key, string schemaPath, out bool ret)
    {
        return TryGetProperty(schemaDefinition, key, BlittableJsonToken.Boolean, schemaPath, out ret);
    }
    
    private static readonly BlittableJsonToken[] StringTypes = [BlittableJsonToken.String, BlittableJsonToken.CompressedString];
    public static bool TryGetString(BlittableJsonReaderObject schemaDefinition, string key, string schemaPath, out string ret)
    {
        return TryGetProperty(schemaDefinition, key, StringTypes, schemaPath, out ret);
    }
    public static bool TryGetInteger(BlittableJsonReaderObject schemaDefinition, string key, string schemaPath, out long ret)
    {
        return TryGetProperty(schemaDefinition, key, BlittableJsonToken.Integer, schemaPath, out ret);
    }
    
    public static bool TryGetObject(BlittableJsonReaderObject schemaDefinition, string key, string schemaPath, out BlittableJsonReaderObject ret)
    {
        return TryGetProperty(schemaDefinition, key, BlittableJsonToken.StartObject, schemaPath, out ret);
    }
    
    public static bool TryGetArray(BlittableJsonReaderObject schemaDefinition, string key, string schemaPath, out BlittableJsonReaderArray ret)
    {
        return TryGetProperty(schemaDefinition, key, BlittableJsonToken.StartArray, schemaPath, out ret);
    }
    
    private static readonly BlittableJsonToken[] NumberTypes = [BlittableJsonToken.LazyNumber, BlittableJsonToken.Integer];
    public static bool TryGetNumber(BlittableJsonReaderObject schemaDefinition, string key, string schemaPath, out decimal ret)
    {
        return TryGetProperty(schemaDefinition, key, NumberTypes, schemaPath, out ret);
    }
    
    public static void TrowRuleTypeError(string rule, object ruleValue, BlittableJsonToken expectedType, BlittableJsonToken actualType, string schemaPath)
    {
        var publicType = GetPublicType(expectedType);
        ThrowRuleTypeError(rule, ruleValue, actualType, schemaPath, publicType);
    }

    private static void ThrowRuleTypeError(string rule, object ruleValue, BlittableJsonToken actualType, string schemaPath, string publicType)
    {
        var type = GetPublicType(actualType);
        ThrowRuleTypeError(rule, ruleValue, publicType, type, schemaPath);
    }
    
    [DoesNotReturn]
    public static void ThrowRuleTypeError(string rule, object ruleValue, string expectedPublicType, string actualPublicType, string schemaPath)
    {
        
        throw new InvalidSchemaValidationDefinitionException(
            $"The value of '{rule}' at '{schemaPath}' must be {GetIndefiniteArticle(expectedPublicType)} {expectedPublicType}, but received '{ruleValue}' of type '{actualPublicType}'.");
    }
    
    public static void ThrowRuleTypeError(string rule, object ruleValue, BlittableJsonToken expectedType, string schemaPath, int index)
    {
        //TODO To maybe merge with other functions
        var actualPublicType = GetPublicType(ruleValue.GetType());
        var expectedPublicType = GetPublicType(expectedType);
        throw new InvalidSchemaValidationDefinitionException(
            $"The value of '{rule}' items at '{schemaPath}' must be {GetIndefiniteArticle(expectedPublicType)} {expectedPublicType}, but received '{ruleValue}' of type '{actualPublicType}' at index '{index}'.");
    }

    private static void TrowRuleTypeError(string rule, object ruleValue, BlittableJsonToken[] expectedTypes, BlittableJsonToken actualType, string schemaPath)
    {
        TrowRuleTypeError(rule, ruleValue, expectedTypes, GetPublicType(actualType), schemaPath);
    }
    
    [DoesNotReturn]
    public static void TrowRuleTypeError(string rule, object ruleValue, BlittableJsonToken[] expectedTypes, string actualPublicType, string schemaPath)
    {
        var publicTypes = expectedTypes.Select(GetPublicType).Distinct().ToArray();
        if (publicTypes.Length == 1)
            ThrowRuleTypeError(rule, ruleValue, publicTypes[0], actualPublicType, schemaPath);
        
        throw new InvalidSchemaValidationDefinitionException(
            $"The value of '{rule}' at '{schemaPath}' must be {string.Join(" or ", publicTypes.Select(x => $"{GetIndefiniteArticle(x)} {x}"))} but received '{ruleValue}' of type '{actualPublicType}'.");
    }

    private static bool TryGetProperty<T>(BlittableJsonReaderObject schemaDefinition,  string rule, BlittableJsonToken expectedType, string schemaPath, out T prefixItems)
    {
        if (TryGetPropertyType(schemaDefinition, rule, out var type) == false)
        {
            prefixItems = default;
            return false;
        }

        if (type != expectedType)
            TrowRuleTypeError(rule, schemaDefinition[rule], expectedType, type, schemaPath);

        if (schemaDefinition.TryGet(rule, out prefixItems) == false)
            throw new InvalidOperationException($"'{rule}' must to convertable to {nameof(BlittableJsonReaderArray)} here. Should not happen");
        
        return true;
    }

    private static bool TryGetProperty<T>(BlittableJsonReaderObject schemaDefinition,  string rule, BlittableJsonToken[] expectedTypes, string schemaPath, out T prefixItems)
    {
        if (TryGetPropertyType(schemaDefinition, rule, out var type) == false)
        {
            prefixItems = default;
            return false;
        }

        if (expectedTypes.Contains(type) == false)
            TrowRuleTypeError(rule, schemaDefinition[rule], expectedTypes, type, schemaPath);

        if (schemaDefinition.TryGet(rule, out prefixItems) == false)
            throw new InvalidOperationException($"'{rule}' must to convertable to {nameof(BlittableJsonReaderArray)} here. Should not happen");
        
        return true;
    }

    private static bool TryGetPropertyType(BlittableJsonReaderObject obj, string prop, out BlittableJsonToken type)
    {
        if(obj.TryGetPropertyType(prop, out type) == false)
            return false;

        type &= BlittableJsonReaderBase.TypesMask;
        return true;
    }
}
