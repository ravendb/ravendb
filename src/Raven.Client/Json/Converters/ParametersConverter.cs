using System;
using Newtonsoft.Json;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Client.Json.Converters
{
    internal sealed class ParametersConverter : RavenJsonConverter
    {
        public static readonly ParametersConverter Instance = new ParametersConverter();

        private ParametersConverter()
        {
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            if (value == null)
            {
                writer.WriteNull();
                return;
            }

            writer.WriteStartObject();

            foreach (var kvp in (Parameters)value)
            {
                writer.WritePropertyName(kvp.Key);

                object v = kvp.Value;
                if (v is DateTime)
                {
                    var dateTime = (DateTime)v;
                    if (dateTime.Kind == DateTimeKind.Unspecified)
                        dateTime = DateTime.SpecifyKind(dateTime, DateTimeKind.Local);
                    writer.WriteValue(dateTime.GetDefaultRavenFormat(dateTime.Kind == DateTimeKind.Utc));
                }
                else if (v is DateTimeOffset)
                {
                    var dateTimeOffset = (DateTimeOffset)v;
                    writer.WriteValue(dateTimeOffset.UtcDateTime.GetDefaultRavenFormat(true));
                }
                else if (v is object[])
                {
                    var oldTypeNameHandling = serializer.TypeNameHandling;

                    try
                    {
                        serializer.TypeNameHandling = TypeNameHandling.None;

                        serializer.Serialize(writer, kvp.Value);
                    }
                    finally
                    {
                        serializer.TypeNameHandling = oldTypeNameHandling;
                    }
                }
                else
                {
                    serializer.Serialize(writer, kvp.Value);
                }
            }

            writer.WriteEndObject();
        }

        public override unsafe object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            if (reader.TokenType == JsonToken.Null)
                return null;

            var result = new Parameters();
            do
            {
                reader.Read();
                if (reader.TokenType == JsonToken.EndObject)
                    return result;
                if (reader.TokenType != JsonToken.PropertyName)
                    throw new InvalidOperationException("Expected PropertyName, Got " + reader.TokenType);

                var key = (string)reader.Value;
                reader.Read();// read the value

                var v = reader.Value;
                if (v is string s)
                {
                    fixed (char* pBuffer = s)
                    {
                        var r = LazyStringParser.TryParseDateTime(pBuffer, s.Length, out var dt, out var dto);
                        switch (r)
                        {
                            case LazyStringParser.Result.Failed:
                                break;
                            case LazyStringParser.Result.DateTime:
                                v = dt;
                                break;
                            case LazyStringParser.Result.DateTimeOffset:
                                v = dto;
                                break;
                            default:
                                throw new ArgumentOutOfRangeException();
                        }
                    }
                }

                result[key] = v;
            } while (true);
        }

        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(Parameters);
        }
    }
}
