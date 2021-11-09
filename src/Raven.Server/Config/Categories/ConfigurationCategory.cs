using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Microsoft.Extensions.Configuration;
using Raven.Client.Extensions;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Raven.Server.ServerWide;
using Sparrow;

namespace Raven.Server.Config.Categories
{
    public abstract class ConfigurationCategory
    {
        private static readonly ConcurrentDictionary<Type, List<ConfigurationProperty>> _configurationPropertiesCache = new ConcurrentDictionary<Type, List<ConfigurationProperty>>();

        public const string DefaultValueSetInConstructor = "default-value-set-in-constructor";

        public class SettingValue
        {
            public SettingValue(string current, bool keyExistsInDatabaseRecord, string server, bool keyExistsInServerSettings)
            {
                CurrentValue = current;
                ServerValue = server;
                KeyExistsInDatabaseRecord = keyExistsInDatabaseRecord;
                KeyExistsInServerSettings = keyExistsInServerSettings;
            }

            public readonly string ServerValue;
            public readonly string CurrentValue;
            public bool KeyExistsInDatabaseRecord;
            public bool KeyExistsInServerSettings;
        }

        protected internal bool Initialized { get; set; }

        protected internal static string GetValue(IConfiguration cfg, string name)
        {
            var section = cfg.GetSection(name);
            if (section.Value != null)
                return section.Value;

            var children = section.GetChildren().ToList();

            return children.Count != 0
                ? string.Join(";", children.Where(x => x.Value != null).Select(x => x.Value))
                : null;
        }

        protected internal static string GetValueForKey(string keyName, IConfiguration cfg)
        {
            var val = GetValue(cfg, keyName);
            if (val != null)
                return val;

            // When setting.json has nested objects, i.e. if 'Path' is nested under 'Security.Certificate'
            // then the key in the IConfiguration object is in the following format: "Security.Certificate:Path"
            // as opposed to: "Security.Certificate.Path" when it is Not nested.
            // Need to look for these variations.

            var sb = new StringBuilder(keyName);

            var lastPeriod = keyName.LastIndexOf('.');
            while (lastPeriod != -1)
            {
                sb[lastPeriod] = ':';
                var tmpName = sb.ToString();
                val = GetValue(cfg, tmpName);
                if (val != null)
                {
                    return val;
                }
                lastPeriod = keyName.LastIndexOf('.', lastPeriod - 1);
            }

            return null;
        }

        public virtual void Initialize(IConfigurationRoot settings, HashSet<string> settingsNames, IConfigurationRoot serverWideSettings, HashSet<string> serverWideSettingsNames, ResourceType type, string resourceName)
        {
            string GetConfigurationValue(IConfigurationRoot cfg, HashSet<string> cfgNames, string keyName, out bool keyExistsInConfiguration)
            {
                keyExistsInConfiguration = false;

                if (cfg == null || keyName == null)
                    return null;

                var val = GetValueForKey(keyName, cfg);

                keyExistsInConfiguration = cfgNames.Contains(keyName) || val != null;

                return val;
            }

            bool keyExistsInDatabaseRecord, keyExistsInServerSettings;

            Initialize(
                key => new SettingValue(GetConfigurationValue(settings, settingsNames, key, out keyExistsInDatabaseRecord), keyExistsInDatabaseRecord,
                        GetConfigurationValue(serverWideSettings, serverWideSettingsNames, key, out keyExistsInServerSettings), keyExistsInServerSettings),
                serverWideSettings?[RavenConfiguration.GetKey(x => x.Core.DataDirectory)], type, resourceName, throwIfThereIsNoSetMethod: true);
        }

        public void Initialize(Func<string, SettingValue> getSetting, string serverDataDir, ResourceType type, string resourceName, bool throwIfThereIsNoSetMethod)
        {
            foreach (var property in GetConfigurationProperties())
            {
                if (property.Info.SetMethod == null)
                {
                    if (throwIfThereIsNoSetMethod)
                        throw new InvalidOperationException($"No set method available for '{property.Info.Name}' property.Info.");

                    continue;
                }

                TimeUnitAttribute timeUnit = property.TimeUnitAttribute;
                SizeUnitAttribute sizeUnit = property.SizeUnitAttribute;

                var configuredValueSet = false;
                var setDefaultValueIfNeeded = true;

                ConfigurationEntryAttribute previousAttribute = null;

                foreach (var entry in property.Info.GetCustomAttributes<ConfigurationEntryAttribute>().OrderBy(order => order.Order))
                {
                    if (previousAttribute != null && previousAttribute.Scope != entry.Scope)
                    {
                        throw new InvalidOperationException($"All ConfigurationEntryAttribute for {property.Info.Name} must have the same Scope");
                    }

                    previousAttribute = entry;

                    var settingValue = getSetting(entry.Key);

                    if (type != ResourceType.Server && entry.Scope == ConfigurationEntryScope.ServerWideOnly && settingValue.CurrentValue != null)
                        throw new InvalidOperationException($"Configuration '{entry.Key}' can only be set at server level.");

                    string value = null;

                    if (settingValue.KeyExistsInDatabaseRecord)
                    {
                        value = settingValue.CurrentValue;

                        if (value == null && property.Info.Type() == typeof(PathSetting))
                        {
                            // for backward compatibility purposes let's ignore null on PathSetting and default to server value - RavenDB-15384
                            value = settingValue.ServerValue;
                        }
                    }
                    else if (settingValue.KeyExistsInServerSettings)
                    {
                        value = settingValue.ServerValue;
                    }

                    setDefaultValueIfNeeded &= entry.SetDefaultValueIfNeeded;

                    if (value == null)
                        continue;

                    value = value.Trim();

                    try
                    {
                        var minValue = property.MinValueAttribute;

                        if (minValue == null)
                        {
                            if (property.Info.PropertyType.IsEnum)
                            {
                                object parsedValue;
                                try
                                {
                                    parsedValue = Enum.Parse(property.Info.PropertyType, value, true);
                                }
                                catch (ArgumentException)
                                {
                                    throw new ConfigurationEnumValueException(value, property.Info.PropertyType);
                                }

                                property.Info.SetValue(this, parsedValue);
                            }
                            else if (property.Info.PropertyType == typeof(string[]))
                            {
                                var values = SplitValue(value);

                                property.Info.SetValue(this, values);
                            }
                            else if (property.Info.PropertyType.IsArray && property.Info.PropertyType.GetElementType().IsEnum)
                            {
                                var values = SplitValue(value)
                                    .Select(item => Enum.Parse(property.Info.PropertyType.GetElementType(), item, ignoreCase: true))
                                    .ToArray();

                                var enumValues = Array.CreateInstance(property.Info.PropertyType.GetElementType(), values.Length);
                                Array.Copy(values, enumValues, enumValues.Length);

                                property.Info.SetValue(this, enumValues);
                            }
                            else if (property.Info.PropertyType == typeof(HashSet<string>))
                            {
                                var hashSet = new HashSet<string>(SplitValue(value), StringComparer.OrdinalIgnoreCase);

                                property.Info.SetValue(this, hashSet);
                            }
                            else if (property.Info.PropertyType == typeof(UriSetting[]))
                            {
                                var values = SplitValue(value);
                                UriSetting[] settings = new UriSetting[values.Length];
                                for (var i = 0; i < values.Length; i++)
                                {
                                    settings[i] = new UriSetting(values[i]);
                                }
                                property.Info.SetValue(this, settings);
                            }
                            else if (timeUnit != null)
                            {
                                property.Info.SetValue(this, new TimeSetting(Convert.ToInt64(value), timeUnit.Unit));
                            }
                            else if (sizeUnit != null)
                            {
                                property.Info.SetValue(this, new Size(Convert.ToInt64(value), sizeUnit.Unit));
                            }
                            else
                            {
                                var t = Nullable.GetUnderlyingType(property.Info.PropertyType) ?? property.Info.PropertyType;

                                if (property.Info.PropertyType == typeof(PathSetting))
                                {
                                    property.Info.SetValue(this,
                                        settingValue.CurrentValue != null
                                            ? new PathSetting(Convert.ToString(value), serverDataDir)
                                            : new PathSetting(Convert.ToString(value), type, resourceName));
                                }
                                else if (property.Info.PropertyType == typeof(PathSetting[]))
                                {
                                    var paths = SplitValue(value);

                                    property.Info.SetValue(this,
                                        settingValue.CurrentValue != null
                                            ? paths.Select(x => new PathSetting(Convert.ToString(x), serverDataDir)).ToArray()
                                            : paths.Select(x => new PathSetting(Convert.ToString(x), type, resourceName)).ToArray());
                                }
                                else if (t == typeof(UriSetting))
                                {
                                    property.Info.SetValue(this, new UriSetting(value));
                                }
                                else
                                {
                                    var safeValue = Convert.ChangeType(value, t);
                                    property.Info.SetValue(this, safeValue);
                                }
                            }
                        }
                        else
                        {
                            if (property.Info.PropertyType == typeof(int) ||
                                property.Info.PropertyType == typeof(int?))
                            {
                                property.Info.SetValue(this, Math.Max(Convert.ToInt32(value), minValue.Int32Value));
                            }
                            else if (property.Info.PropertyType == typeof(Size) ||
                                     property.Info.PropertyType == typeof(Size?))
                            {
                                property.Info.SetValue(this, new Size(Math.Max(Convert.ToInt32(value), minValue.Int32Value), sizeUnit.Unit));
                            }
                            else if (property.Info.PropertyType == typeof(TimeSetting) ||
                                     property.Info.PropertyType == typeof(TimeSetting?))
                            {
                                property.Info.SetValue(this, new TimeSetting(Math.Max(Convert.ToInt32(value), minValue.Int32Value), timeUnit.Unit));
                            }
                            else
                            {
                                throw new NotSupportedException("Min value for " + property.Info.PropertyType + " is not supported. Property name: " + property.Info.Name);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException($"Could not set '{entry.Key}' configuration setting value.", e);
                    }

                    configuredValueSet = true;
                    break;
                }

                if (configuredValueSet || setDefaultValueIfNeeded == false)
                    continue;

                var defaultValueAttribute = property.DefaultValueAttribute;

                if (defaultValueAttribute == null)
                {
                    throw new InvalidOperationException($"Property '{property.Info.Name}' does not have a default value attribute");
                }

                var defaultValue = defaultValueAttribute.Value;

                if (DefaultValueSetInConstructor.Equals(defaultValue))
                    continue;

                if (timeUnit != null && defaultValue != null)
                {
                    property.Info.SetValue(this, new TimeSetting(Convert.ToInt64(defaultValue), timeUnit.Unit));
                }
                else if (sizeUnit != null && defaultValue != null)
                {
                    property.Info.SetValue(this, new Size(Convert.ToInt64(defaultValue), sizeUnit.Unit));
                }
                else
                {
                    if (property.Info.PropertyType == typeof(PathSetting) && defaultValue != null)
                    {
                        property.Info.SetValue(this, new PathSetting(Convert.ToString(defaultValue), type, resourceName));
                    }
                    else if (property.Info.PropertyType == typeof(string[]) && defaultValue is string defaultValueAsString1)
                    {
                        var values = SplitValue(defaultValueAsString1);
                        property.Info.SetValue(this, values);
                    }
                    else if (property.Info.PropertyType.IsArray && property.Info.PropertyType.GetElementType().IsEnum && defaultValue is string defaultValueAsString2)
                    {
                        var values = SplitValue(defaultValueAsString2)
                            .Select(item => Enum.Parse(property.Info.PropertyType.GetElementType(), item, ignoreCase: true))
                            .ToArray();

                        var enumValues = Array.CreateInstance(property.Info.PropertyType.GetElementType(), values.Length);
                        Array.Copy(values, enumValues, enumValues.Length);

                        property.Info.SetValue(this, enumValues);
                    }
                    else
                        property.Info.SetValue(this, defaultValue);
                }
            }

            Initialized = true;
        }

        protected virtual void ValidateProperty(PropertyInfo property)
        {
            var defaultValueAttribute = property.GetCustomAttribute<DefaultValueAttribute>();
            if (defaultValueAttribute == null)
                ThrowMissingDefaultValue(property);
        }

        protected static void ThrowMissingDefaultValue(PropertyInfo property)
        {
            throw new InvalidOperationException($"The {nameof(DefaultValueAttribute)} is missing for '{property.Name}' property.");
        }

        protected List<ConfigurationProperty> GetConfigurationProperties()
        {
            var configurationProperties = _configurationPropertiesCache.GetOrAdd(GetType(), GetConfigurationPropertiesInternal);

            return configurationProperties;
        }

        private List<ConfigurationProperty> GetConfigurationPropertiesInternal(Type type)
        {
            var configurationProperties = from property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                                          let configurationEntryAttribute = property.GetCustomAttributes<ConfigurationEntryAttribute>().FirstOrDefault()
                                          where configurationEntryAttribute != null // filter out properties which aren't marked as configuration entry
                                          orderby configurationEntryAttribute.Order // properties are initialized in order of declaration
                                          select property;

            var results = new List<ConfigurationProperty>();

            foreach (var property in configurationProperties)
            {
                ValidateProperty(property);

                var attributes = property.GetCustomAttributes<ConfigurationEntryAttribute>().OrderBy(order => order.Order).ToList();
                var defaultValueAttribute = property.GetCustomAttribute<DefaultValueAttribute>();
                var minValueAttribute = property.GetCustomAttribute<MinValueAttribute>();

                TimeUnitAttribute timeUnit = null;
                SizeUnitAttribute sizeUnit = null;

                if (property.PropertyType == typeof(TimeSetting) ||
                    property.PropertyType == typeof(TimeSetting?))
                {
                    timeUnit = property.GetCustomAttribute<TimeUnitAttribute>();
                    Debug.Assert(timeUnit != null);
                }
                else if (property.PropertyType == typeof(Size) ||
                         property.PropertyType == typeof(Size?))
                {
                    sizeUnit = property.GetCustomAttribute<SizeUnitAttribute>();
                    Debug.Assert(sizeUnit != null);
                }

                var item = new ConfigurationProperty
                {
                    Info = property,
                    ConfigurationEntryAttributes = attributes,
                    DefaultValueAttribute = defaultValueAttribute,
                    MinValueAttribute = minValueAttribute,
                    TimeUnitAttribute = timeUnit,
                    SizeUnitAttribute = sizeUnit
                };

                results.Add(item);
            }

            return results;
        }

        public object GetDefaultValue<T>(Expression<Func<T, object>> getValue)
        {
            var prop = getValue.ToProperty();
            var value = prop.GetCustomAttributes<DefaultValueAttribute>().First().Value;

            if (DefaultValueSetInConstructor.Equals(value))
            {
                return prop.GetValue(this);
            }

            return value;
        }

        private static string[] SplitValue(string value)
        {
            var values = value.Split(';', StringSplitOptions.RemoveEmptyEntries);
            for (var i = 0; i < values.Length; i++)
                values[i] = values[i].Trim();
            return values;
        }

        public class ConfigurationProperty
        {
            public PropertyInfo Info;

            public List<ConfigurationEntryAttribute> ConfigurationEntryAttributes;

            public DefaultValueAttribute DefaultValueAttribute;

            public MinValueAttribute MinValueAttribute;

            public TimeUnitAttribute TimeUnitAttribute;

            public SizeUnitAttribute SizeUnitAttribute;
        }
    }
}
