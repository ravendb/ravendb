using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.Extensions.Configuration;
using Raven.Abstractions.Extensions;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Raven.Server.ServerWide;

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
            Initialize(key => new SettingValue(settings[key], serverWideSettings?[key]), type, resourceName, throwIfThereIsNoSetMethod: true);
        }

        public void Initialize(Func<string, SettingValue> getSetting, ResourceType type, string resourceName, bool throwIfThereIsNoSetMethod)
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

                if (property.PropertyType == TimeSetting.TypeOf || property.PropertyType == TimeSetting.NullableTypeOf)
                {
                    timeUnit = property.GetCustomAttribute<TimeUnitAttribute>();
                    Debug.Assert(timeUnit != null);
                }
                else if (property.PropertyType == Size.TypeOf || property.PropertyType == Size.NullableTypeOf)
                {
                    sizeUnit = property.GetCustomAttribute<SizeUnitAttribute>();
                    Debug.Assert(sizeUnit != null);
                }

                var configuredValueSet = false;
                var setDefaultValueOfNeeded = true;

                foreach (var entry in property.GetCustomAttributes<ConfigurationEntryAttribute>())
                {
                    var settingValue = getSetting(entry.Key);
                    var value = settingValue.CurrentValue ?? settingValue.ServerValue;
                    setDefaultValueOfNeeded &= entry.SetDefaultValueIfNeeded;

                    if (value == null)
                        continue;

                    try
                    {
                        var minValue = property.GetCustomAttribute<MinValueAttribute>();

                        if (minValue == null)
                        {
                            if (property.PropertyType.GetTypeInfo().IsEnum)
                            {
                                property.SetValue(this, Enum.Parse(property.PropertyType, value, true));
                            }
                            if (property.PropertyType == typeof(string[]))
                            {
                                var values = value.Split(';');
                                property.SetValue(this, values);
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
                                    if (settingValue.CurrentValue != null)
                                        property.SetValue(this, new PathSetting(Convert.ToString(value)));
                                    else
                                        property.SetValue(this, new PathSetting(Convert.ToString(value), type, resourceName));
                                }
                                else if (property.PropertyType == typeof(PathSetting[]))
                                {
                                    var paths = value.Split(';');

                                    if (settingValue.CurrentValue != null)
                                        property.SetValue(this, paths.Select(x => new PathSetting(Convert.ToString(x))).ToArray());
                                    else
                                        property.SetValue(this, paths.Select(x => new PathSetting(Convert.ToString(x), type, resourceName)).ToArray());
                                }
                                else
                                {
                                    var safeValue = (value == null) ? null : Convert.ChangeType(value, t);
                                    property.SetValue(this, safeValue);
                                }
                                
                            }
                        }
                        else
                        {
                            if (property.PropertyType == typeof(int) || property.PropertyType == typeof(int?))
                            {
                                property.SetValue(this, Math.Max(Convert.ToInt32(value), minValue.Int32Value));
                            }
                            else if (property.PropertyType == Size.TypeOf)
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
                        throw new InvalidOperationException("Could not set configuration value given under the following setting: " + entry.Key, e);
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

        protected object GetDefaultValue<T>(Expression<Func<T, object>> getValue)
        {
            var prop = getValue.ToProperty();
            var value = prop.GetCustomAttributes<DefaultValueAttribute>().First().Value;

            if (DefaultValueSetInConstructor.Equals(value))
            {
                return prop.GetValue(this);
            }

            return value;
        }
    }
}