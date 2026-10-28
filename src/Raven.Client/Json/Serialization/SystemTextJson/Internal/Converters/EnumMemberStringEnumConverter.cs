using System;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters
{
    internal sealed class EnumMemberStringEnumConverter : JsonConverterFactory
    {
        public static readonly EnumMemberStringEnumConverter Instance = new();

        private static readonly JsonStringEnumConverter DefaultConverter = new();

        private EnumMemberStringEnumConverter()
        {
        }

        public override bool CanConvert(Type typeToConvert)
        {
            return typeToConvert.IsEnum;
        }

        public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
        {
            Dictionary<string, string> values = GetEnumMemberValues(typeToConvert);
            if (values.Count == 0)
                return DefaultConverter.CreateConverter(typeToConvert, options);

            Type factoryType = typeof(JsonStringEnumConverter<>).MakeGenericType(typeToConvert);
            var factory = (JsonConverterFactory)Activator.CreateInstance(factoryType, new EnumMemberNamingPolicy(values), true);
            return factory.CreateConverter(typeToConvert, options);
        }

        private static Dictionary<string, string> GetEnumMemberValues(Type enumType)
        {
            var values = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (FieldInfo field in enumType.GetFields(BindingFlags.Public | BindingFlags.Static))
            {
                string value = field.GetCustomAttribute<EnumMemberAttribute>()?.Value;
                if (value != null)
                    values[field.Name] = value;
            }

            return values;
        }

        private sealed class EnumMemberNamingPolicy : JsonNamingPolicy
        {
            private readonly Dictionary<string, string> _values;

            public EnumMemberNamingPolicy(Dictionary<string, string> values)
            {
                _values = values;
            }

            public override string ConvertName(string name)
            {
                return _values.TryGetValue(name, out string value) ? value : name;
            }
        }
    }
}
