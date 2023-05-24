using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Reflection;
using Newtonsoft.Json;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow;
using Sparrow.Utils;
using System.Linq;

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

        private enum ParameterType
        {
            Unknown = 0,
            DateTime,
            DateTimeOffset,
            Enumerable,
#if FEATURE_DATEONLY_TIMEONLY_SUPPORT
            DateOnly,
            TimeOnly,
#endif
            RavenAssembly,
        }

        private static readonly TypeCache<ParameterType> ConverterCache;

        static ParametersConverter()
        {
            ConverterCache = new(256);
            ConverterCache.Put(typeof(DateTime), ParameterType.DateTime);
            ConverterCache.Put(typeof(DateTimeOffset), ParameterType.DateTimeOffset);
#if FEATURE_DATEONLY_TIMEONLY_SUPPORT
            ConverterCache.Put(typeof(DateOnly), ParameterType.DateOnly);
            ConverterCache.Put(typeof(TimeOnly), ParameterType.TimeOnly);
#endif
        }

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
                    if (v == null)
                    {
                        serializer.Serialize(writer, null);
                    }
                    else
                    {
                        var vType = v.GetType();
                        if (ConverterCache.TryGet(vType, out var pType) == false)
                        {
                            // Ensure that we figure out which converter we need. 
                            if (v is IEnumerable)
                            {
                                pType = ParameterType.Enumerable;
                            }
                            else if (IsRavenAssembly(v))
                            {
                                pType = ParameterType.RavenAssembly;
                            }
                            else
                            {
                                pType = ParameterType.Unknown;
                            }

                            ConverterCache.Put(vType, pType);
                        }

                        switch (pType)
                        {
                            case ParameterType.Unknown:
                                serializer.Serialize(writer, v);
                                break;
                            case ParameterType.DateTime:
                                var dateTime = (DateTime)v;
                                if (dateTime.Kind == DateTimeKind.Unspecified)
                                    dateTime = DateTime.SpecifyKind(dateTime, DateTimeKind.Local);
                                writer.WriteValue(dateTime.GetDefaultRavenFormat());
                                break;
                            case ParameterType.DateTimeOffset:
                                writer.WriteValue(((DateTimeOffset)v).UtcDateTime.GetDefaultRavenFormat(true));
                                break;
                            case ParameterType.Enumerable:
                                var oldTypeNameHandling = serializer.TypeNameHandling;
                                try
                                {
                                    serializer.TypeNameHandling = Newtonsoft.Json.TypeNameHandling.None;
                                    serializer.Serialize(writer, (IEnumerable)v);
                                }
                                finally
                                {
                                    serializer.TypeNameHandling = oldTypeNameHandling;
                                }
                                break;
#if FEATURE_DATEONLY_TIMEONLY_SUPPORT
                            case ParameterType.DateOnly:
                                writer.WriteValue(((DateOnly)v).ToString(DefaultFormat.DateOnlyFormatToWrite, CultureInfo.InvariantCulture));
                                break;
                            case ParameterType.TimeOnly:
                                writer.WriteValue(((TimeOnly)v).ToString(DefaultFormat.TimeOnlyFormatToWrite, CultureInfo.InvariantCulture));
                                break;
#endif
                            case ParameterType.RavenAssembly:
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
                                break;
                        }
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
                        var r = LazyStringParser.TryParseTimeForQuery(pBuffer, s.Length, out var dt, out var dto,
#if FEATURE_DATEONLY_TIMEONLY_SUPPORT
                            out var @do,
                            out var to,
#endif
                            properlyParseThreeDigitsMilliseconds: true);
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
#if FEATURE_DATEONLY_TIMEONLY_SUPPORT
                            case LazyStringParser.Result.DateOnly:
                                v = @do;
                                break;
                            case LazyStringParser.Result.TimeOnly:
                                v = to;
                                break;
#endif
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
