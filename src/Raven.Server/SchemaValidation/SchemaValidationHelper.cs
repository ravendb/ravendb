using System;
using System.Collections.Generic;
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
    
    public static bool TryConvertTypeToTokens(IComparable<string> type, out BlittableJsonToken[] tokens)
    {
        return StringTypeToBlittableToken.TryGetValue(type, out tokens);
    }

    public static string GetPublicTypeOfObj(object obj) => GetPublicType(obj?.GetType());

    public static string GetPublicType(Type type)
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
        if (type == BlittableJsonToken.Null)
            return Null;
        if (type == BlittableJsonToken.Integer)
            return Integer;
        if (type == BlittableJsonToken.LazyNumber)
            return Number;
        if (type is BlittableJsonToken.String or BlittableJsonToken.CompressedString)
            return String;
        if (type == BlittableJsonToken.Boolean)
            return Boolean;
        if (type == BlittableJsonToken.StartObject)
            return Object;
        if (type == BlittableJsonToken.StartArray)
            return Array;

        throw new NotImplementedException($"The type '{type}' is not supported.");
    }
}
