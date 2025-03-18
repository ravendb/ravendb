using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Categories;
using Raven.Server.Config.Settings;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json.Parsing;

namespace Raven.Server.Config
{
    public sealed class ConfigurationEntryMetadata : IDynamicJson
    {
        public readonly string Category;

        public readonly string[] Keys;

        public ConfigurationEntryScope Scope;

        public readonly string DefaultValue;

        public readonly bool IsDefaultValueDynamic;

        public readonly string Description;

        public readonly ConfigurationEntryType Type;

        public SizeUnit? SizeUnit { get; private set; }

        public TimeUnit? TimeUnit { get; private set; }

        public int? MinValue { get; private set; }

        public bool IsArray { get; private set; }

        public bool IsDictionary { get; private set; }

        public bool IsNullable { get; private set; }

        public bool IsSecured { get; private set; }

        public string[] AvailableValues { get; private set; }

        public ConfigurationEntryMetadata(PropertyInfo configurationCategoryProperty, PropertyInfo configurationProperty, RavenConfiguration configuration)
        {
            if (configurationCategoryProperty is null)
                throw new ArgumentNullException(nameof(configurationCategoryProperty));
            if (configurationProperty is null)
                throw new ArgumentNullException(nameof(configurationProperty));

            var keys = new List<string>();
            foreach (var configurationEntry in configurationProperty.GetCustomAttributes<ConfigurationEntryAttribute>(inherit: true).OrderBy(x => x.Order))
            {
                keys.Add(configurationEntry.Key);

                Scope = configurationEntry.Scope;
                IsSecured = configurationEntry.IsSecured;
            }

            if (keys.Count == 0)
                throw new InvalidOperationException($"Property '{configurationProperty.Name}' does not have any configuration entry attributes.");

            Keys = keys.ToArray();
            Description = configurationProperty.GetCustomAttribute<DescriptionAttribute>(inherit: true)?.Description;
            Type = GetConfigurationEntryType(configurationProperty);
            DefaultValue = GetDefaultValue(configurationCategoryProperty, configurationProperty, configuration, out IsDefaultValueDynamic);

            var categoryType = configurationCategoryProperty.PropertyType.GetCustomAttribute<ConfigurationCategoryAttribute>(inherit: true);
            if (categoryType == null)
                throw new InvalidOperationException($"Category '{configurationCategoryProperty.PropertyType.Name}' does not have any configuration category attributes.");

            Category = categoryType.Type.GetDescription();
        }

        internal bool IsMatch(string key)
        {
            foreach (var k in Keys)
            {
                if (string.Equals(k, key, StringComparison.OrdinalIgnoreCase))
                    return true;
            }

            return false;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Category)] = Category,
                [nameof(Keys)] = new DynamicJsonArray(Keys),
                [nameof(Scope)] = Scope,
                [nameof(DefaultValue)] = DefaultValue,
                [nameof(IsDefaultValueDynamic)] = IsDefaultValueDynamic,
                [nameof(Description)] = Description,
                [nameof(Type)] = Type,
                [nameof(SizeUnit)] = SizeUnit,
                [nameof(TimeUnit)] = TimeUnit,
                [nameof(MinValue)] = MinValue,
                [nameof(IsArray)] = IsArray,
                [nameof(IsNullable)] = IsNullable,
                [nameof(IsSecured)] = IsSecured,
                [nameof(AvailableValues)] = AvailableValues != null ? new DynamicJsonArray(AvailableValues) : null
            };
        }

        private string GetDefaultValue(PropertyInfo configurationCategoryProperty, PropertyInfo configurationProperty, RavenConfiguration configuration, out bool isDefaultValueDynamic)
        {
            isDefaultValueDynamic = false;
            var defaultValue = configurationProperty.GetCustomAttribute<DefaultValueAttribute>(inherit: true)?.Value?.ToString();
            if (defaultValue == ConfigurationCategory.DefaultValueSetInConstructor)
            {
                isDefaultValueDynamic = true;

                var configurationCategory = configurationCategoryProperty.GetValue(configuration);
                var configurationValue = configurationProperty.GetValue(configurationCategory);
                if (configurationValue == null)
                    return null;

                switch (Type)
                {
                    case ConfigurationEntryType.String:
                    case ConfigurationEntryType.Boolean:
                    case ConfigurationEntryType.Enum:
                    case ConfigurationEntryType.Uri:
                    case ConfigurationEntryType.Path:
                    case ConfigurationEntryType.Integer:
                    case ConfigurationEntryType.Double:
                        return configurationValue.ToString();
                    case ConfigurationEntryType.Size:
                        var configurationValueAsSize = (Size)configurationValue;
                        return configurationValueAsSize.GetValue(SizeUnit.Value).ToString();
                    case ConfigurationEntryType.Time:
                        var configurationValueAsTime = (TimeSetting)configurationValue;
                        return configurationValueAsTime.GetValueAsString(TimeUnit.Value);
                    default:
                        throw new NotSupportedException($"Type '{Type}' is not supported.");
                }
            }

            return defaultValue;
        }

        private ConfigurationEntryType GetConfigurationEntryType(PropertyInfo property)
        {
            var type = property.PropertyType;

            if (CalculateIsDictionary(Keys[0], type, out var elementType))
            {
                type = elementType;
                IsDictionary = true;
            }
            else if (CalculateIsArray(Keys[0], type, out elementType))
            {
                type = elementType;
                IsArray = true;
            }

            var nullableType = Nullable.GetUnderlyingType(type);
            if (nullableType != null)
            {
                type = nullableType;
                IsNullable = true;
            }

            var minValue = property.GetCustomAttribute<MinValueAttribute>(inherit: true);
            if (minValue != null)
                MinValue = minValue.Int32Value;

            if (type == typeof(string))
                return ConfigurationEntryType.String;
            if (type == typeof(int) || type == typeof(long))
                return ConfigurationEntryType.Integer;
            if (type == typeof(float) || type == typeof(double))
                return ConfigurationEntryType.Double;
            if (type == typeof(bool))
                return ConfigurationEntryType.Boolean;
            if (type.IsEnum)
            {
                AvailableValues = type.GetEnumNames();

                return ConfigurationEntryType.Enum;
            }
            if (type == typeof(UriSetting))
                return ConfigurationEntryType.Uri;
            if (type == typeof(PathSetting))
                return ConfigurationEntryType.Path;
            if (type == typeof(Size))
            {
                var sizeUnit = property.GetCustomAttribute<SizeUnitAttribute>(inherit: true);
                if (sizeUnit == null)
                    throw new InvalidOperationException($"Following key '{Keys[0]}' doesn't have required size unit attribute.");

                SizeUnit = sizeUnit.Unit;
                return ConfigurationEntryType.Size;
            }
            if (type == typeof(TimeSetting))
            {
                var timeUnit = property.GetCustomAttribute<TimeUnitAttribute>(inherit: true);
                if (timeUnit == null)
                    throw new InvalidOperationException($"Following key '{Keys[0]}' doesn't have required time unit attribute.");

                TimeUnit = timeUnit.Unit;
                return ConfigurationEntryType.Time;
            }

            if (type == typeof(KeyValuePair<string, string>))
                return ConfigurationEntryType.Dictionary;

            throw new NotSupportedException($"Key: {Keys[0]}. Type: {type.FullName}");

            static bool CalculateIsDictionary(string key, Type typeToCheck, out Type elementType)
            {
                elementType = null;

                if (typeToCheck == typeof(Dictionary<string, string>))
                {
                    elementType = typeof(KeyValuePair<string, string>);
                    return true;
                }

                return false;
            }

            static bool CalculateIsArray(string key, Type typeToCheck, out Type elementType)
            {
                elementType = null;

                if (typeToCheck.IsArray)
                {
                    elementType = typeToCheck.GetElementType();
                    return true;
                }

                if (typeToCheck.IsGenericType)
                {
                    var genericType = typeToCheck.GetGenericTypeDefinition();
                    if (genericType.GetInterfaces().Any(x => x.IsAssignableFrom(typeof(IEnumerable<>))))
                    {
                        var genericTypeArguments = typeToCheck.GetGenericArguments();
                        if (genericTypeArguments.Length != 1)
                            throw new InvalidOperationException($"Following key '{key}' contains more than one generic argument.");

                        elementType = genericTypeArguments[0];
                        return true;
                    }
                }

                return false;
            }
        }
    }

    public enum ConfigurationEntryType
    {
        Unknown,
        String,
        Boolean,
        Enum,
        Uri,
        Path,
        Size,
        Time,
        Integer,
        Double,
        Dictionary
    }
}
