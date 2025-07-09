using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using Raven.Server.Exceptions.SchemaValidation;
using Raven.Server.SchemaValidation.ErrorMessage;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation;

public static class SchemaValidationHelper
{
    public const string Null = "null";
    public const string Integer = "integer";
    public const string Number = "number";
    public const string String = "string";
    public const string Boolean = "boolean";
    public const string Object = "object";
    public const string Array = "array";

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

    public static bool TryGetTypesForPublicType(IComparable<string> type, out Type[] types)
    {
        if (type.CompareTo(Null) == 0)
        {
            types = [null];
            return true;
        }

        if (type.CompareTo(Integer) == 0)
        {
            types = [typeof(long)];
            return true;
        }

        if (type.CompareTo(Number) == 0)
        {
            types = [typeof(long), typeof(LazyNumberValue)];
            return true;
        }

        if (type.CompareTo(String) == 0)
        {
            types = [typeof(LazyStringValue), typeof(LazyCompressedStringValue)];
            return true;
        }

        if (type.CompareTo(Boolean) == 0)
        {
            types = [typeof(bool)];
            return true;
        }

        if (type.CompareTo(Object) == 0)
        {
            types = [typeof(BlittableJsonReaderObject)];
            return true;
        }

        if (type.CompareTo(Array) == 0)
        {
            types = [typeof(BlittableJsonReaderArray)];
            return true;
        }

        types = null;
        return false;
    }

    public static string GetPublicTypeOfObj(object obj) => GetPublicType(obj?.GetType());

    public static string GetPublicType(Type type)
    {
        if (type == null)
            return Null;
        if (type == typeof(long))
            return Integer;
        if (type == typeof(LazyNumberValue) || type == typeof(decimal))
            return Number;
        if (type == typeof(LazyStringValue) || type == typeof(LazyCompressedStringValue))
            return String;
        if (type == typeof(bool))
            return Boolean;
        if (type == typeof(BlittableJsonReaderObject))
            return Object;
        if (type == typeof(BlittableJsonReaderArray))
            return Array;

        Debug.Assert(false, $"The type '{type.FullName}' is not supported.");
        return type.Name;
    }

    private static string GetIndefiniteArticle(string word)
    {
        if (string.IsNullOrEmpty(word))
            throw new ArgumentException("Input word cannot be null or empty.");

        return "aeiouAEIOU".Contains(word[0]) ? "an" : "a";
    }

    public static bool TryGetBoolean(BlittableJsonReaderObject schemaDefinition, string key, SchemaPath schemaPath, out bool ret)
    {
        return TryGetProperty(schemaDefinition, key, schemaPath, out ret);
    }

    private static readonly HashSet<Type> StringTypes = [typeof(LazyStringValue), typeof(LazyCompressedStringValue)];
    public static bool TryGetString(BlittableJsonReaderObject schemaDefinition, string key, SchemaPath schemaPath, out string ret)
    {
        return TryGetProperty(schemaDefinition, key, StringTypes, schemaPath, out ret);
    }

    public static bool TryGetInteger(BlittableJsonReaderObject schemaDefinition, string key, SchemaPath schemaPath, out long ret)
    {
        return TryGetProperty(schemaDefinition, key, schemaPath, out ret);
    }

    public static bool TryGetObject(BlittableJsonReaderObject schemaDefinition, string key, SchemaPath schemaPath, out BlittableJsonReaderObject ret)
    {
        return TryGetProperty(schemaDefinition, key, schemaPath, out ret);
    }

    public static bool TryGetArray(BlittableJsonReaderObject schemaDefinition, string key, SchemaPath schemaPath, out BlittableJsonReaderArray ret)
    {
        return TryGetProperty(schemaDefinition, key, schemaPath, out ret);
    }

    private static readonly HashSet<Type> NumberTypes = (new Type[] {typeof(LazyNumberValue), typeof(long)}).ToHashSet();

    //TODO Maybe to use LazyNumberValue instead of decimal
    public static bool TryGetNumber(BlittableJsonReaderObject schemaDefinition, string key, SchemaPath schemaPath, out decimal ret)
    {
        return TryGetProperty(schemaDefinition, key, NumberTypes, schemaPath, out ret);
    }

    [DoesNotReturn]
    public static void ThrowRuleTypeError(object ruleValue, Type expectedType, SchemaPath schemaPath)
    {
        var expectedPublicType = GetPublicType(expectedType);
        throw new InvalidSchemaValidationDefinitionException(
            $"The value of '{schemaPath.Property}' must be {GetIndefiniteArticle(expectedPublicType)} {expectedPublicType}, but received '{ruleValue}' of type '{GetPublicTypeOfObj(ruleValue)}'. Schema path '{schemaPath.FullPath}'.");
    }

    [DoesNotReturn]
    public static void ThrowRuleTypeError(object ruleValue, HashSet<Type> expectedTypes, SchemaPath schemaPath)
    {
        if (expectedTypes.Count == 1)
            ThrowRuleTypeError(ruleValue, expectedTypes.First(), schemaPath);

        var expectedPublicType = expectedTypes.Select(GetPublicType).Distinct();
        throw new InvalidSchemaValidationDefinitionException(
            $"The value of '{schemaPath.Property}' must be {string.Join(" or ", expectedPublicType.Select(x => $"{GetIndefiniteArticle(x)} {x}"))}, but received '{ruleValue}' of type '{GetPublicType(ruleValue.GetType())}'. Schema path '{schemaPath.FullPath}'.");
    }

    private static bool TryGetProperty<T>(BlittableJsonReaderObject schemaDefinition, string rule, SchemaPath schemaPath, out T value)
    {
        if (schemaDefinition.TryGetMember(new StringSegment(rule), out var objValue) == false)
        {
            value = default;
            return false;
        }

        value = CheckTypeAndThrow<T>(objValue, schemaPath);
        return true;
    }

    public static T CheckTypeAndThrow<T>(object objValue, SchemaPath schemaPath)
    {
        if (objValue is T tValue)
            return tValue;

        ThrowRuleTypeError(objValue, typeof(T), schemaPath);
        return default; // Required to satisfy compiler flow analysis; method above always throws
    }
    
    public static T[] CheckBlittableArrayElementTypesAndThrow<T>(BlittableJsonReaderArray objValue, SchemaPath schemaPath)
    {
        var blittableArray = CheckTypeAndThrow<BlittableJsonReaderArray>(objValue, schemaPath);
        List<T> ret = null;
        for (int i = 0; i < blittableArray.Length; i++)
        {
            var item = CheckTypeAndThrow<T>(blittableArray[i], schemaPath + i);
            (ret ??= []).Add(item);
        }

        return ret?.ToArray();
    }

    private static bool TryGetProperty<T>(BlittableJsonReaderObject schemaDefinition, string rule, HashSet<Type> expectedTypes, SchemaPath schemaPath, out T value)
    {
        if (schemaDefinition.TryGetMember(new StringSegment(rule), out var oValue) == false)
        {
            value = default;
            return false;
        }

        if (expectedTypes.Contains(oValue.GetType()) == false)
            ThrowRuleTypeError(schemaDefinition[rule], expectedTypes, schemaPath);

        if (schemaDefinition.TryGet(rule, out value) == false)
            throw new InvalidOperationException($"'{rule}' must to convertable to {nameof(T)} here. Should not happen");

        return true;
    }
}
