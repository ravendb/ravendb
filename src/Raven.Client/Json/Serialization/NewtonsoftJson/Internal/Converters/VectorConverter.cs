using System;
using System.Collections;
using System.IO;
using System.Numerics;
using Newtonsoft.Json;
using Raven.Client.Documents;

namespace Raven.Client.Json.Serialization.NewtonsoftJson.Internal.Converters;

internal sealed class VectorConverter : JsonConverter
{
    public static readonly VectorConverter Instance = new();
    
    private static bool TryWriteVectorArray<T>(JsonWriter writer, RavenVector<T> vector)
    where T : unmanaged
#if NET7_0_OR_GREATER
    , INumber<T>
#endif
    {
        foreach (T element in vector.Embedding)
            WriteValue(element);
        
        return true;
        
        void WriteValue(in T value)
        {    
            if (typeof(T) == typeof(float))
                writer.WriteValue((float)(object)value); 
            else if (typeof(T) == typeof(double))
                writer.WriteValue((double)(object)value); 
            else if (typeof(T) == typeof(byte))
                writer.WriteValue((byte)(object)value); 
            else if (typeof(T) == typeof(ushort))
                writer.WriteValue((ushort)(object)value); 
            else if (typeof(T) == typeof(uint))
                writer.WriteValue((uint)(object)value); 
            else if (typeof(T) == typeof(ulong))
                writer.WriteValue((ulong)(object)value); 
            else if (typeof(T) == typeof(sbyte))
                writer.WriteValue((sbyte)(object)value);  
            else if (typeof(T) == typeof(short))
                writer.WriteValue((short)(object)value); 
            else if (typeof(T) == typeof(int))
                writer.WriteValue((int)(object)value);  
            else if (typeof(T) == typeof(long))
                writer.WriteValue((long)(object)value);
            else
                writer.WriteValue(value);
        }
    }
    
    public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
    {
        if (value == null)
        {
            writer.WriteNull();
            return;
        }
        
        writer.WriteStartObject();
        writer.WritePropertyName("@vector");
        writer.WriteStartArray();

        // For known types we can cast vector, otherwise we will use reflection
        var writtenKnownType = value switch
        {
            RavenVector<byte> v => TryWriteVectorArray(writer, v),
            RavenVector<ushort> v => TryWriteVectorArray(writer, v),
            RavenVector<uint> v => TryWriteVectorArray(writer, v),
            RavenVector<ulong> v => TryWriteVectorArray(writer, v),
            RavenVector<sbyte> v => TryWriteVectorArray(writer, v),
            RavenVector<short> v => TryWriteVectorArray(writer, v),
            RavenVector<int> v => TryWriteVectorArray(writer, v),
            RavenVector<long> v => TryWriteVectorArray(writer, v),
            RavenVector<decimal> v => TryWriteVectorArray(writer, v),
            _ => false
        };

        if (writtenKnownType)
        {
            IEnumerable enumerable = value as IEnumerable;
            
            if (enumerable is null)
                throw new InvalidDataException($"Expected IEnumerable, but got {enumerable} instead.");
            
            foreach (var element in enumerable)
                writer.WriteValue(element);
        }
        
        writer.WriteEndArray();
        
        writer.WriteEndObject();
    }
    
    public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
    {
        if (reader.TokenType == JsonToken.Null)
            return null;
        
        if (reader.TokenType != JsonToken.PropertyName)
            throw new InvalidOperationException();
        
        reader.Read();
        
        return reader.Value;
    }

    public override bool CanConvert(Type objectType)
    {
        if (objectType.IsGenericType == false)
            return false;

        return objectType.GetGenericTypeDefinition() == typeof(RavenVector<>);
    }
}
