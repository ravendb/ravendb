using System;
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
        public const string DefaultValueSetInConstructor = "default-value-set-in-constructor";

        public class SettingValue
        {
            public SettingValue(string current, string server)
            {
                CurrentValue = current;
                ServerValue = server;
            }

            public readonly string ServerValue;

            public readonly string CurrentValue;
        }

        protected internal bool Initialized { get; set; }

        public virtual void Initialize(IConfigurationRoot settings, IConfigurationRoot serverWideSettings, ResourceType type, string resourceName)
        {
            string GetValue(IConfiguration cfg, string name)
            {
                var section = cfg.GetSection(name);
                if (section == null) 
                    return null;

                if (section.Value != null)
                    return section.Value;

                var children = section.GetChildren().ToList();

                return children.Count != 0 
                    ? string.Join(";", children.Where(x => x.Value != null).Select(x => x.Value)) 
                    : null;
            }

            string GetConfiguration(IConfiguration cfg, string name)
            {
                if (cfg == null || name == null)
                    return null;

                var val = GetValue(cfg, name);
                if (val != null)
                    return val;

                var sb = new StringBuilder(name);

                var lastPeriod = name.LastIndexOf('.');
                while (lastPeriod != -1)
                {
                    sb[lastPeriod] = ':';
                    var tmpName = sb.ToString();
                    val = GetValue(cfg, tmpName);
                    if (val != null)
                        return val;
                    lastPeriod = name.LastIndexOf('.', lastPeriod - 1);
                }

                return null;
            }

            Initialize(key => new SettingValue(GetConfiguration(settings, key), GetConfiguration(serverWideSettings, key)),
                serverWideSettings?[RavenConfiguration.GetKey(x => x.Core.DataDirectory)], type, resourceName,
                throwIfThereIsNoSetMethod: true);
        }

        public void Initialize(Func<string, SettingValue> getSetting, string serverDataDir, ResourceType type, string resourceName, bool throwIfThereIsNoSetMethod)
        {
            foreach (var property in GetConfigurationProperties())
            {
                if (property.SetMethod == null)
                {
                    if (throwIfThereIsNoSetMethod)
                        throw new InvalidOperationException($"No set method available for '{property.Name}' property.");

                    continue;
                }

                ValidateProperty(property);

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

                var configuredValueSet = false;
                var setDefaultValueOfNeeded = true;

                ConfigurationEntryAttribute previousAttribute = null;

                foreach (var entry in property.GetCustomAttributes<ConfigurationEntryAttribute>().OrderBy(order => order.Order))
                {
                    if (previousAttribute != null && previousAttribute.Scope != entry.Scope)
                    {
                        throw new InvalidOperationException($"All ConfigurationEntryAttribute for {property.Name} must have the same Scope");
                    }

                    previousAttribute = entry;
                    var settingValue = getSetting(entry.Key);
                    if (type != ResourceType.Server && entry.Scope == ConfigurationEntryScope.ServerWideOnly && settingValue.CurrentValue != null)
                        throw new InvalidOperationException($"Configuration '{entry.Key}' can only be set at server level.");

                    var value = settingValue.CurrentValue ?? settingValue.ServerValue;
                    setDefaultValueOfNeeded &= entry.SetDefaultValueIfNeeded;

                    if (value == null)
                        continue;

                    value = value.Trim();

                    try
                    {
                        var minValue = property.GetCustomAttribute<MinValueAttribute>();

                        if (minValue == null)
                        {
                            if (property.PropertyType.GetTypeInfo().IsEnum)
                            {
                                object parsedValue;
                                try
                                {
                                    parsedValue = Enum.Parse(property.PropertyType, value, true);
                                }
                                catch (ArgumentException)
                                {
                                    throw new ConfigurationEnumValueException(value, property.PropertyType);
                                }

                                property.SetValue(this, parsedValue);
                            }
                            else if (property.PropertyType == typeof(string[]))
                            {
                                var values = SplitValue(value);

                                property.SetValue(this, values);
                            }
                            else if (property.PropertyType == typeof(HashSet<string>))
                            {
                                var hashSet = new HashSet<string>(SplitValue(value), StringComparer.OrdinalIgnoreCase);

                                property.SetValue(this, hashSet);
                            }
                            else if (timeUnit != null)
                            {
                                property.SetValue(this, new TimeSetting(Convert.ToInt64(value), timeUnit.Unit));
                            }
                            else if (sizeUnit != null)
                            {
                                property.SetValue(this, new Size(Convert.ToInt64(value), sizeUnit.Unit));
                            }
                            else
                            {
                                var t = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;

                                if (property.PropertyType == typeof(PathSetting))
                                {
                                    property.SetValue(this,
                                        settingValue.CurrentValue != null
                                            ? new PathSetting(Convert.ToString(value), serverDataDir)
                                            : new PathSetting(Convert.ToString(value), type, resourceName));
                                }
                                else if (property.PropertyType == typeof(PathSetting[]))
                                {
                                    var paths = SplitValue(value);

                                    property.SetValue(this,
                                        settingValue.CurrentValue != null
                                            ? paths.Select(x => new PathSetting(Convert.ToString(x), serverDataDir)).ToArray()
                                            : paths.Select(x => new PathSetting(Convert.ToString(x), type, resourceName)).ToArray());
                                }
                                else if (t == typeof(UriSetting))
                                {
                                    property.SetValue(this, new UriSetting(value));
                                }
                                else
                                {
                                    var safeValue = Convert.ChangeType(value, t);
                                    property.SetValue(this, safeValue);
                                }
                            }
                        }
                        else
                        {
                            if (property.PropertyType == typeof(int) ||
                                property.PropertyType == typeof(int?))
                            {
                                property.SetValue(this, Math.Max(Convert.ToInt32(value), minValue.Int32Value));
                            }
                            else if (property.PropertyType == typeof(Size) ||
                                     property.PropertyType == typeof(Size?))
                            {
                                property.SetValue(this, new Size(Math.Max(Convert.ToInt32(value), minValue.Int32Value), sizeUnit.Unit));
                            }
                            else
                            {
                                throw new NotSupportedException("Min value for " + property.PropertyType + " is not supported. Property name: " + property.Name);
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

                if (configuredValueSet || setDefaultValueOfNeeded == false)
                    continue;

                var defaultValueAttribute = property.GetCustomAttribute<DefaultValueAttribute>();

                if (defaultValueAttribute == null)
                {
                    throw new InvalidOperationException($"Property '{property.Name}' does not have a default value attribute");
                }

                var defaultValue = defaultValueAttribute.Value;

                if (DefaultValueSetInConstructor.Equals(defaultValue))
                    continue;

                if (timeUnit != null && defaultValue != null)
                {
                    property.SetValue(this, new TimeSetting(Convert.ToInt64(defaultValue), timeUnit.Unit));
                }
                else if (sizeUnit != null && defaultValue != null)
                {
                    property.SetValue(this, new Size(Convert.ToInt64(defaultValue), sizeUnit.Unit));
                }
                else
                {
                    if (property.PropertyType == typeof(PathSetting) && defaultValue != null)
                    {
                        property.SetValue(this, new PathSetting(Convert.ToString(defaultValue), type, resourceName));
                    }
                    else if (property.PropertyType == typeof(string[]) && defaultValue is string defaultValueAsString)
                    {
                        var values = SplitValue(defaultValueAsString);
                        property.SetValue(this, values);
                    }
                    else
                        property.SetValue(this, defaultValue);
                }
            }

            Initialized = true;
        }

        protected virtual void ValidateProperty(PropertyInfo property)
        {
        }

        protected IEnumerable<PropertyInfo> GetConfigurationProperties()
        {
            var configurationProperties = from property in GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance)
                                          let configurationEntryAttribute = property.GetCustomAttributes<ConfigurationEntryAttribute>().FirstOrDefault()
                                          where configurationEntryAttribute != null // filter out properties which aren't marked as configuration entry
                                          orderby configurationEntryAttribute.Order // properties are initialized in order of declaration
                                          select property;

            return configurationProperties;
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
    }
}
