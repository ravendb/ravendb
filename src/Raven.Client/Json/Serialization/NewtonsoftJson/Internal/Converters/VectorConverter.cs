using System;
using System.Collections;
using System.Diagnostics;
using System.IO;
using System.Numerics;
using System.Reflection;
using System.Reflection.Emit;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Sparrow.Json;

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
            else if (typeof(T) == typeof(decimal))
                writer.WriteValue((decimal)(object)value);
            
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
        writer.WritePropertyName(Sparrow.Global.Constants.Naming.VectorPropertyName);
        writer.WriteStartArray();

        // For known types we can cast vector, otherwise we will use reflection
        var writtenKnownType = value switch
        {
            RavenVector<float> v => TryWriteVectorArray(writer, v),
            RavenVector<double> v => TryWriteVectorArray(writer, v),
            RavenVector<decimal> v => TryWriteVectorArray(writer, v),
            
            RavenVector<byte> v => TryWriteVectorArray(writer, v),
            RavenVector<ushort> v => TryWriteVectorArray(writer, v),
            RavenVector<uint> v => TryWriteVectorArray(writer, v),
            RavenVector<ulong> v => TryWriteVectorArray(writer, v),
            
            RavenVector<sbyte> v => TryWriteVectorArray(writer, v),
            RavenVector<short> v => TryWriteVectorArray(writer, v),
            RavenVector<int> v => TryWriteVectorArray(writer, v),
            RavenVector<long> v => TryWriteVectorArray(writer, v),
            _ => false
        };

        if (writtenKnownType == false)
        {
            IEnumerable enumerable = value as IEnumerable;
            
            if (enumerable is null)
                throw new InvalidDataException($"Expected IEnumerable, but got {value} instead.");
            
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
        
        Debug.Assert(reader.TokenType == JsonToken.StartObject, "reader.TokenType == JsonToken.StartArray");
        
        reader.Read();
        Debug.Assert(reader.TokenType == JsonToken.PropertyName, "reader.TokenType == JsonToken.PropertyName");
        
        Debug.Assert(reader.Value?.ToString() == Sparrow.Global.Constants.Naming.VectorPropertyName, "propertyName == VectorPropertyName");

        // Read vector value
        reader.Read();

        Debug.Assert(objectType.GenericTypeArguments.Length == 1);
        
        var embeddingType = objectType.GenericTypeArguments[0];
        
        var blittableJsonReaderVector = (BlittableJsonReaderVector)reader.Value;

        reader.Read();
        Debug.Assert(reader.TokenType == JsonToken.EndObject, "reader.TokenType == JsonToken.EndObject");
        
        if (embeddingType == typeof(float))
            return CreateRavenVector<float>(blittableJsonReaderVector);
        if (embeddingType == typeof(double))
            return CreateRavenVector<double>(blittableJsonReaderVector);
        if (embeddingType == typeof(decimal))
            return CreateRavenVector<double>(blittableJsonReaderVector);
        
        if (embeddingType == typeof(byte))
            return CreateRavenVector<byte>(blittableJsonReaderVector);
        if (embeddingType == typeof(ushort))
            return CreateRavenVector<ushort>(blittableJsonReaderVector);
        if (embeddingType == typeof(uint))
            return CreateRavenVector<uint>(blittableJsonReaderVector);
        if (embeddingType == typeof(ulong))
            return CreateRavenVector<ulong>(blittableJsonReaderVector);
        
        if (embeddingType == typeof(sbyte))
            return CreateRavenVector<sbyte>(blittableJsonReaderVector);
        if (embeddingType == typeof(short))
            return CreateRavenVector<short>(blittableJsonReaderVector);
        if (embeddingType == typeof(int))
            return CreateRavenVector<int>(blittableJsonReaderVector);
        if (embeddingType == typeof(long))
            return CreateRavenVector<long>(blittableJsonReaderVector);

        throw new InvalidOperationException($"Type {embeddingType} is not supported.");
    }

    private static RavenVector<T> CreateRavenVector<T>(BlittableJsonReaderVector bjrv) where T : unmanaged
#if NET7_0_OR_GREATER
        , INumber<T>
#endif
    {
        var vectorSpan = bjrv.ReadArray<T>();
        return new RavenVector<T>(vectorSpan.ToArray());
    }

    public override bool CanConvert(Type objectType)
    {
        if (objectType.IsGenericType == false)
            return false;

        return objectType.GetGenericTypeDefinition() == typeof(RavenVector<>);
    }
}
