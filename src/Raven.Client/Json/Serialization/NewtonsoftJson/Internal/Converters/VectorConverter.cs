using System;
using System.Collections;
using System.Diagnostics;
using System.Numerics;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization.NewtonsoftJson.Internal.Converters;

internal sealed class VectorConverter : JsonConverter
{
    public static readonly VectorConverter Instance = new();
    
    /// <summary>
    /// When the writer is a BlittableJsonWriter, we can simply use the native blittable Vector writer.
    /// Otherwise, we need to construct a JSON array of numerical values.
    /// </summary>
    private static bool CanWriteVectorInNativeForm(JsonWriter writer) => writer is BlittableJsonWriter;
    
    private static void WriteVectorArray<T>(JsonWriter writer, RavenVector<T> vector, bool writeInNativeForm) where T : unmanaged
    #if NET7_0_OR_GREATER
    , INumber<T>
    #endif
    {
        if (writeInNativeForm)
        {
            ((BlittableJsonWriter)writer).WriteVector<T>(vector.Embedding);
        }
        else
        {
            // very unlikely, rollback for the default serializer
            foreach (T value in vector.Embedding)
            {
                if (typeof(T) == typeof(float))
                    writer.WriteValue((float)(object)value); 
                else if (typeof(T) == typeof(double))
                    writer.WriteValue((double)(object)value); 
                else if (typeof(T) == typeof(decimal))
                    writer.WriteValue((decimal)(object)value);
#if NET6_0_OR_GREATER
                else if (typeof(T) == typeof(Half))
                    writer.WriteValue((Half)(object)value);
#endif
                
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
        
        bool canWriteInNativeForm = CanWriteVectorInNativeForm(writer);
        
        if (canWriteInNativeForm == false)
            writer.WriteStartArray();

        // For known types we can cast vector, otherwise we will use reflection
        switch (value)
        {
            case RavenVector<float> v:
                WriteVectorArray(writer, v, canWriteInNativeForm);
                break;
            case RavenVector<double> v:
                WriteVectorArray(writer, v, canWriteInNativeForm);
                break;
            case RavenVector<decimal> v:
                WriteVectorArray(writer, v, canWriteInNativeForm);
                break;
            
            case RavenVector<byte> v:
                WriteVectorArray(writer, v, canWriteInNativeForm);
                break;
            case RavenVector<ushort> v:
                WriteVectorArray(writer, v, canWriteInNativeForm);
                break;
            case RavenVector<uint> v:
                WriteVectorArray(writer, v, canWriteInNativeForm);
                break;
            case RavenVector<ulong> v:
                WriteVectorArray(writer, v, canWriteInNativeForm);
                break;
            case RavenVector<sbyte> v:
                WriteVectorArray(writer, v, canWriteInNativeForm);
                break;
            case RavenVector<short> v:
                WriteVectorArray(writer, v, canWriteInNativeForm);
                break;
            case RavenVector<int> v:
                WriteVectorArray(writer, v, canWriteInNativeForm);
                break;
            case RavenVector<long> v:
                WriteVectorArray(writer, v, canWriteInNativeForm);
                break;
#if NET6_0_OR_GREATER
            case RavenVector<Half> v:
                WriteVectorArray(writer, v, canWriteInNativeForm);
                break;
#endif
            default:
                if (canWriteInNativeForm)
                    writer.WriteStartArray();
                canWriteInNativeForm = false;
                foreach (var element in (IEnumerable)value)
                    writer.WriteValue(element);
                break;
                
        }
        
        if (canWriteInNativeForm == false)
            writer.WriteEndArray();
        
        writer.WriteEndObject();
    }
    
    public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
    {
        if (reader.TokenType == JsonToken.Null)
            return null;
        
        Debug.Assert(reader.TokenType == JsonToken.StartObject, "reader.TokenType == JsonToken.StartArray");
        
        // Reading the "@vector" property
        reader.Read();
        Debug.Assert(reader.TokenType == JsonToken.PropertyName, "reader.TokenType == JsonToken.PropertyName");
        Debug.Assert(reader.Value?.ToString() == Sparrow.Global.Constants.Naming.VectorPropertyName, "propertyName == VectorPropertyName");
        
        // Read array
        reader.Read();
        Debug.Assert(objectType.GenericTypeArguments.Length == 1);
        var embeddingType = objectType.GenericTypeArguments[0];
        var blittableJsonReaderVector = (BlittableJsonReaderVector)reader.Value;
        
        reader.Read(); //end object
        Debug.Assert(reader.TokenType == JsonToken.EndObject, "reader.TokenType == JsonToken.EndObject");
        
        if (embeddingType == typeof(float))
            return CreateRavenVector<float>(blittableJsonReaderVector);
        if (embeddingType == typeof(double))
            return CreateRavenVector<double>(blittableJsonReaderVector);
        if (embeddingType == typeof(decimal))
            return CreateRavenVector<double>(blittableJsonReaderVector);
#if NET6_0_OR_GREATER
        if (embeddingType == typeof(Half))
            return CreateRavenVector<Half>(blittableJsonReaderVector);
#endif
        
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
        if (bjrv.TryReadArray<T>(out var vectorSpan))
            return new RavenVector<T>(vectorSpan.ToArray());

        //when we compressed the type internally, we've to fallback and read one-by-one
        var enumerator = bjrv.ReadAs<T>();
        var vector = new T[enumerator.Count];
        var idX = 0;
        while (enumerator.MoveNext())
            vector[idX++] = enumerator.Current;
        
        Debug.Assert(idX == vector.Length);
        return new RavenVector<T>(vector);
    }

    public override bool CanConvert(Type objectType)
    {
        if (objectType.IsGenericType == false)
            return false;

        return objectType.GetGenericTypeDefinition() == typeof(RavenVector<>);
    }
}
