using System;
using System.Collections.Generic;
using Microsoft.Extensions.Primitives;
using Newtonsoft.Json;

namespace Raven.Client.Json.Converters
{
    internal class JsonObjectConverter : RavenJsonConverter
    {
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            var sv = (StringValues)value;
            switch (sv.Count)
            {
                case 0:
                    writer.WriteNull();
                    break;
                case 1:
                    writer.WriteValue(sv[0]);
                    break;
                default:
                    writer.WriteStartArray();
                    foreach (var v in sv)
                    {
                        writer.WriteValue(v);
                    }
                    writer.WriteEndArray();
                    break;
            }
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            switch (reader.TokenType)
            {
                case JsonToken.Null:
                    return new StringValues();
                case JsonToken.String:
                    return reader.Value;
                case JsonToken.StartArray:
                    var list = new List<string>();
                    while (reader.Read())
                    {
                        if (reader.TokenType == JsonToken.EndArray)
                            return list.ToArray();
                        list.Add((string) reader.Value);
                    }
                    return list.ToArray();
                default:
                    throw new ArgumentOutOfRangeException(nameof(reader), reader.TokenType.ToString());
            }
        }

        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(StringValues);
        }
    }
}