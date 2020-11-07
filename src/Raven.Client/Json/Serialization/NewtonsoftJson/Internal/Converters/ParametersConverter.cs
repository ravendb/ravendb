using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using Newtonsoft.Json;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization.NewtonsoftJson.Internal.Converters
{
    internal sealed class ParametersConverter : JsonConverter
    {
        public static readonly ParametersConverter Instance = new ParametersConverter();

        private static readonly HashSet<Assembly> RavenAssemblies = new HashSet<Assembly>
        {
            typeof(ParametersConverter).Assembly,
            typeof(LazyStringValue).Assembly
        };

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

            var oldPreserveReferencesHandling = serializer.PreserveReferencesHandling;

            try
            {
                serializer.PreserveReferencesHandling = PreserveReferencesHandling.None;

                foreach (var kvp in (Parameters)value)
                {
                    writer.WritePropertyName(kvp.Key);

                    var v = kvp.Value;

                    if (v is DateTime dateTime)
                    {
                        if (dateTime.Kind == DateTimeKind.Unspecified)
                            dateTime = DateTime.SpecifyKind(dateTime, DateTimeKind.Local);
                        writer.WriteValue(dateTime.GetDefaultRavenFormat());
                    }
                    else if (v is DateTimeOffset dateTimeOffset)
                    {
                        writer.WriteValue(dateTimeOffset.UtcDateTime.GetDefaultRavenFormat(true));
                    }
                    else if (v is IEnumerable enumerable)
                    {
                        var oldTypeNameHandling = serializer.TypeNameHandling;

                        try
                        {
                            serializer.TypeNameHandling = Newtonsoft.Json.TypeNameHandling.None;

                            serializer.Serialize(writer, enumerable);
                        }
                        finally
                        {
                            serializer.TypeNameHandling = oldTypeNameHandling;
                        }
                    }
                    else if (IsRavenAssembly(v))
                    {
                        var oldNullValueHandling = serializer.NullValueHandling;
                        var oldDefaultValueHandling = serializer.DefaultValueHandling;

                        try
                        {
                            serializer.NullValueHandling = NullValueHandling.Ignore;
                            serializer.DefaultValueHandling = DefaultValueHandling.Ignore;

                            serializer.Serialize(writer, v);
                        }
                        finally
                        {
                            serializer.NullValueHandling = oldNullValueHandling;
                            serializer.DefaultValueHandling = oldDefaultValueHandling;
                        }
                    }
                    else
                    {
                        serializer.Serialize(writer, v);
                    }
                }
            }
            finally
            {
                serializer.PreserveReferencesHandling = oldPreserveReferencesHandling;
            }

            writer.WriteEndObject();
        }

        private static bool IsRavenAssembly(object item)
        {
            if (item == null)
                return false;

            return RavenAssemblies.Contains(item.GetType().Assembly);
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
