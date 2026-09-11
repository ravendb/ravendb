using System;
using System.Collections.Generic;
using System.Numerics;
using System.Text.Json;
using System.Text.Json.Serialization;
using Raven.Client.Documents;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters;

internal sealed class VectorConverter : JsonConverterFactory
{
    public static readonly VectorConverter Instance = new();

    private VectorConverter()
    {
    }

    public override bool CanConvert(Type typeToConvert)
    {
        if (typeToConvert.IsGenericType == false)
            return false;

        return typeToConvert.GetGenericTypeDefinition() == typeof(RavenVector<>);
    }

    public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
    {
        Type elementType = typeToConvert.GetGenericArguments()[0];

        if (elementType == typeof(float))
            return VectorConverterInner<float>.Instance;
        if (elementType == typeof(double))
            return VectorConverterInner<double>.Instance;
        if (elementType == typeof(decimal))
            return VectorConverterInner<decimal>.Instance;
        if (elementType == typeof(byte))
            return VectorConverterInner<byte>.Instance;
        if (elementType == typeof(ushort))
            return VectorConverterInner<ushort>.Instance;
        if (elementType == typeof(uint))
            return VectorConverterInner<uint>.Instance;
        if (elementType == typeof(ulong))
            return VectorConverterInner<ulong>.Instance;
        if (elementType == typeof(sbyte))
            return VectorConverterInner<sbyte>.Instance;
        if (elementType == typeof(short))
            return VectorConverterInner<short>.Instance;
        if (elementType == typeof(int))
            return VectorConverterInner<int>.Instance;
        if (elementType == typeof(long))
            return VectorConverterInner<long>.Instance;
#if NET6_0_OR_GREATER
        if (elementType == typeof(Half))
            return VectorConverterInner<Half>.Instance;
#endif

        throw new InvalidOperationException($"Type {elementType.FullName} is not supported for RavenVector<T>.");
    }

    private sealed class VectorConverterInner<T> : JsonConverter<RavenVector<T>>
        where T : unmanaged
#if NET7_0_OR_GREATER
        , INumber<T>
#endif
    {
        public static readonly VectorConverterInner<T> Instance = new();

        private VectorConverterInner()
        {
        }

        public override RavenVector<T> Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.Null)
                return null;

            if (reader.TokenType != JsonTokenType.StartObject)
                throw new JsonException($"Expected StartObject token, got {reader.TokenType}.");

            reader.Read(); // move to property name
            if (reader.TokenType != JsonTokenType.PropertyName)
                throw new JsonException($"Expected PropertyName token, got {reader.TokenType}.");

            string propertyName = reader.GetString();
            if (propertyName != Sparrow.Global.Constants.Naming.VectorPropertyName)
                throw new JsonException($"Expected property '{Sparrow.Global.Constants.Naming.VectorPropertyName}', got '{propertyName}'.");

            reader.Read(); // move to array start
            if (reader.TokenType != JsonTokenType.StartArray)
                throw new JsonException($"Expected StartArray token, got {reader.TokenType}.");

            List<T> values = new List<T>();
            while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                values.Add(ReadElement(ref reader));

            reader.Read(); // move past EndObject
            if (reader.TokenType != JsonTokenType.EndObject)
                throw new JsonException($"Expected EndObject token, got {reader.TokenType}.");

            return new RavenVector<T>(values.ToArray());
        }

        private static T ReadElement(ref Utf8JsonReader reader)
        {
            if (typeof(T) == typeof(float))
                return (T)(object)reader.GetSingle();
            if (typeof(T) == typeof(double))
                return (T)(object)reader.GetDouble();
            if (typeof(T) == typeof(decimal))
                return (T)(object)reader.GetDecimal();
            if (typeof(T) == typeof(byte))
                return (T)(object)reader.GetByte();
            if (typeof(T) == typeof(ushort))
                return (T)(object)reader.GetUInt16();
            if (typeof(T) == typeof(uint))
                return (T)(object)reader.GetUInt32();
            if (typeof(T) == typeof(ulong))
                return (T)(object)reader.GetUInt64();
            if (typeof(T) == typeof(sbyte))
                return (T)(object)reader.GetSByte();
            if (typeof(T) == typeof(short))
                return (T)(object)reader.GetInt16();
            if (typeof(T) == typeof(int))
                return (T)(object)reader.GetInt32();
            if (typeof(T) == typeof(long))
                return (T)(object)reader.GetInt64();
#if NET6_0_OR_GREATER
            if (typeof(T) == typeof(Half))
                return (T)(object)(Half)reader.GetSingle();
#endif

            throw new InvalidOperationException($"Type {typeof(T).FullName} is not supported for RavenVector<T>.");
        }

        public override void Write(Utf8JsonWriter writer, RavenVector<T> value, JsonSerializerOptions options)
        {
            if (value == null)
            {
                writer.WriteNullValue();
                return;
            }

            writer.WriteStartObject();
            writer.WritePropertyName(Sparrow.Global.Constants.Naming.VectorPropertyName);
            writer.WriteStartArray();

            foreach (T element in value.Embedding)
                WriteElement(writer, element);

            writer.WriteEndArray();
            writer.WriteEndObject();
        }

        private static void WriteElement(Utf8JsonWriter writer, T value)
        {
            if (typeof(T) == typeof(float))
                writer.WriteNumberValue((float)(object)value);
            else if (typeof(T) == typeof(double))
                writer.WriteNumberValue((double)(object)value);
            else if (typeof(T) == typeof(decimal))
                writer.WriteNumberValue((decimal)(object)value);
            else if (typeof(T) == typeof(byte))
                writer.WriteNumberValue((byte)(object)value);
            else if (typeof(T) == typeof(ushort))
                writer.WriteNumberValue((ushort)(object)value);
            else if (typeof(T) == typeof(uint))
                writer.WriteNumberValue((uint)(object)value);
            else if (typeof(T) == typeof(ulong))
                writer.WriteNumberValue((ulong)(object)value);
            else if (typeof(T) == typeof(sbyte))
                writer.WriteNumberValue((sbyte)(object)value);
            else if (typeof(T) == typeof(short))
                writer.WriteNumberValue((short)(object)value);
            else if (typeof(T) == typeof(int))
                writer.WriteNumberValue((int)(object)value);
            else if (typeof(T) == typeof(long))
                writer.WriteNumberValue((long)(object)value);
#if NET6_0_OR_GREATER
            else if (typeof(T) == typeof(Half))
                writer.WriteNumberValue((float)(Half)(object)value);
#endif
            else
                throw new InvalidOperationException($"Type {typeof(T).FullName} is not supported for RavenVector<T>.");
        }
    }
}
