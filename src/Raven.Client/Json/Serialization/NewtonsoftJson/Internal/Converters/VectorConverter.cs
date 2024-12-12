using System;
using System.Numerics;
using Newtonsoft.Json;
using Raven.Client.Documents;

namespace Raven.Client.Json.Serialization.NewtonsoftJson.Internal.Converters;

internal sealed class VectorConverter : JsonConverter
{
    public static readonly VectorConverter Instance = new();
    
    public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
    {
        if (value == null)
        {
            writer.WriteNull();
            return;
        }
        
        writer.WriteStartObject();
        
        writer.WritePropertyName("@vector");
        
        // todo
        var vector = (RavenVector<float>)value;
        
        writer.WriteStartArray();

        foreach (var element in vector.Embedding)
            writer.WriteValue(element);
        
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
