using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using Sparrow;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters
{
    internal sealed class ParametersConverter : JsonConverter<Parameters>
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

        public override bool CanConvert(Type typeToConvert)
        {
            return typeToConvert == typeof(Parameters);
        }

        public override Parameters Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            throw new NotSupportedException($"{nameof(ParametersConverter)} does not support reading.");
        }

        public override void Write(Utf8JsonWriter writer, Parameters value, JsonSerializerOptions options)
        {
            if (value == null)
            {
                writer.WriteNullValue();
                return;
            }

            writer.WriteStartObject();

            foreach (KeyValuePair<string, object> kvp in value)
            {
                writer.WritePropertyName(kvp.Key);

                object v = kvp.Value;
                if (v == null)
                {
                    writer.WriteNullValue();
                }
                else
                {
                    Type vType = v.GetType();
                    if (ConverterCache.TryGet(vType, out ParameterType pType) == false)
                    {
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
                            JsonSerializer.Serialize(writer, v, vType, options);
                            break;
                        case ParameterType.DateTime:
                            DateTime dateTime = (DateTime)v;
                            if (dateTime.Kind == DateTimeKind.Unspecified)
                                dateTime = DateTime.SpecifyKind(dateTime, DateTimeKind.Local);
                            writer.WriteStringValue(dateTime.GetDefaultRavenFormat());
                            break;
                        case ParameterType.DateTimeOffset:
                            writer.WriteStringValue(((DateTimeOffset)v).UtcDateTime.GetDefaultRavenFormat(true));
                            break;
                        case ParameterType.Enumerable:
                            JsonSerializer.Serialize(writer, v, vType, options);
                            break;
#if FEATURE_DATEONLY_TIMEONLY_SUPPORT
                        case ParameterType.DateOnly:
                            writer.WriteStringValue(((DateOnly)v).ToString(DefaultFormat.DateOnlyFormatToWrite, CultureInfo.InvariantCulture));
                            break;
                        case ParameterType.TimeOnly:
                            writer.WriteStringValue(((TimeOnly)v).ToString(DefaultFormat.TimeOnlyFormatToWrite, CultureInfo.InvariantCulture));
                            break;
#endif
                        case ParameterType.RavenAssembly:
                            JsonSerializerOptions ravenOptions = GetRavenAssemblyOptions(options);
                            JsonSerializer.Serialize(writer, v, vType, ravenOptions);
                            break;
                    }
                }
            }

            writer.WriteEndObject();
        }

        private static bool IsRavenAssembly(object item)
        {
            if (item == null)
                return false;

            return RavenAssemblies.Contains(item.GetType().Assembly);
        }

        private static JsonSerializerOptions _ravenAssemblyOptions;

        private static JsonSerializerOptions GetRavenAssemblyOptions(JsonSerializerOptions baseOptions)
        {
            JsonSerializerOptions existing = _ravenAssemblyOptions;
            if (existing != null)
                return existing;

            JsonSerializerOptions created = new JsonSerializerOptions(baseOptions)
            {
                DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingDefault
            };

            _ravenAssemblyOptions = created;
            return created;
        }
    }
}
