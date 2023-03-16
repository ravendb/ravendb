using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Categories;
using Raven.Server.Config.Settings;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues;

public class RavenDB_20092 : NoDisposalNeeded
{
    private static readonly Dictionary<TimeUnit, string> TimeSettingSuffixes = new Dictionary<TimeUnit, string>
    {
        { TimeUnit.Days, "InDays" },
        { TimeUnit.Hours, "InHrs" },
        { TimeUnit.Milliseconds, "InMs" },
        { TimeUnit.Minutes, "InMin" },
        { TimeUnit.Seconds, "InSec" },
    };

    private static readonly HashSet<string> NotAllowedTimeSettingPropertySuffixes;

    private static readonly HashSet<string> TimeSettingConfigurationEntriesToIgnore = new()
    {
        "Indexing.MaxTimeToWaitAfterFlushAndSyncWhenExceedingScratchSpaceLimit",
        "Security.AuditLog.RetentionTimeInHours"
    };

    private static readonly Dictionary<SizeUnit, string> SizeSuffixes = new Dictionary<SizeUnit, string>
    {
        { SizeUnit.Bytes, "InBytes" },
        { SizeUnit.Kilobytes, "InKb" },
        { SizeUnit.Megabytes, "InMb" },
        { SizeUnit.Gigabytes, "InGb" },
        { SizeUnit.Terabytes, "InTb" },
    };

    private static readonly HashSet<string> NotAllowedSizePropertySuffixes;

    private static readonly HashSet<string> SizeConfigurationEntriesToIgnore = new()
    {
        "Http.MinDataRateBytesPerSec"
    };

    static RavenDB_20092()
    {
        NotAllowedTimeSettingPropertySuffixes = new HashSet<string>();

        foreach (var kvp in TimeSettingSuffixes)
        {
            NotAllowedTimeSettingPropertySuffixes.Add(kvp.Key.ToString());
            NotAllowedTimeSettingPropertySuffixes.Add(kvp.Value);
        }

        NotAllowedSizePropertySuffixes = new HashSet<string>();

        foreach (var kvp in SizeSuffixes)
        {
            NotAllowedSizePropertySuffixes.Add(kvp.Key.ToString());
            NotAllowedSizePropertySuffixes.Add(kvp.Value);
        }
    }

    public RavenDB_20092(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void TimeSetting_Conventions_For_Configuration()
    {
        var categories = typeof(ConfigurationCategory).Assembly
            .GetTypes()
            .Where(x => x.IsSubclassOf(typeof(ConfigurationCategory)))
            .Where(x => x.IsAbstract == false)
            .ToList();

        var settingsChecked = 0;

        foreach (var category in categories)
        {
            foreach (var property in category.GetProperties(BindingFlags.Instance | BindingFlags.Public))
            {
                var propertyType = property.PropertyType;
                if (propertyType != typeof(TimeSetting) && propertyType != typeof(TimeSetting?))
                    continue;

                var propertyName = property.Name;
                if (NotAllowedTimeSettingPropertySuffixes.Any(x => propertyName.EndsWith(x)))
                    throw new InvalidOperationException($"Property '{propertyName}' in category '{category.Name}' ends with invalid suffix. The name of the property should not indicate an unit, because TimeSetting already represents a TimeSpan.");

                var timeUnitAttribute = property.GetCustomAttribute<TimeUnitAttribute>();
                if (timeUnitAttribute == null)
                    throw new InvalidOperationException($"Property '{propertyName}' in '{category.Name}' does not have a '{nameof(TimeUnitAttribute)}'.");

                var configurationEntryAttributes = property.GetCustomAttributes<ConfigurationEntryAttribute>().ToList();
                if (configurationEntryAttributes == null || configurationEntryAttributes.Count == 0)
                    throw new InvalidOperationException($"Property '{propertyName}' in '{category.Name}' does not have a '{nameof(ConfigurationEntryAttribute)}'.");

                var configurationEntrySuffix = TimeSettingSuffixes[timeUnitAttribute.Unit];

                foreach (var configurationEntryAttribute in configurationEntryAttributes)
                {
                    var key = configurationEntryAttribute.Key;
                    if (TimeSettingConfigurationEntriesToIgnore.Contains(key))
                        continue;

                    if (key.EndsWith(configurationEntrySuffix) == false)
                        throw new InvalidOperationException($"Configuration entry '{key}' for property '{propertyName}' in '{category.Name}' ends with invalid suffix. Required suffix is '{configurationEntrySuffix}'.");
                }

                settingsChecked++;
            }
        }

        Assert.True(settingsChecked > 0, $"{settingsChecked} > 0");
    }

    [Fact]
    public void Size_Conventions_For_Configuration()
    {
        var categories = typeof(ConfigurationCategory).Assembly
            .GetTypes()
            .Where(x => x.IsSubclassOf(typeof(ConfigurationCategory)))
            .Where(x => x.IsAbstract == false)
            .ToList();

        var settingsChecked = 0;

        foreach (var category in categories)
        {
            foreach (var property in category.GetProperties(BindingFlags.Instance | BindingFlags.Public))
            {
                var propertyType = property.PropertyType;
                if (propertyType != typeof(Size) && propertyType != typeof(Size?))
                    continue;

                var propertyName = property.Name;
                if (NotAllowedSizePropertySuffixes.Any(x => propertyName.EndsWith(x)))
                    throw new InvalidOperationException($"Property '{propertyName}' in category '{category.Name}' ends with invalid suffix. The name of the property should not indicate an unit, because Size already represents that.");

                var sizeUnitAttribute = property.GetCustomAttribute<SizeUnitAttribute>();
                if (sizeUnitAttribute == null)
                    throw new InvalidOperationException($"Property '{propertyName}' in '{category.Name}' does not have a '{nameof(SizeUnitAttribute)}'.");

                var configurationEntryAttributes = property.GetCustomAttributes<ConfigurationEntryAttribute>().ToList();
                if (configurationEntryAttributes == null || configurationEntryAttributes.Count == 0)
                    throw new InvalidOperationException($"Property '{propertyName}' in '{category.Name}' does not have a '{nameof(ConfigurationEntryAttribute)}'.");

                var configurationEntrySuffix = SizeSuffixes[sizeUnitAttribute.Unit];

                foreach (var configurationEntryAttribute in configurationEntryAttributes)
                {
                    var key = configurationEntryAttribute.Key;
                    if (SizeConfigurationEntriesToIgnore.Contains(key))
                        continue;

                    if (key.EndsWith(configurationEntrySuffix) == false)
                        throw new InvalidOperationException($"Configuration entry '{key}' for property '{propertyName}' in '{category.Name}' ends with invalid suffix. Required suffix is '{configurationEntrySuffix}'.");
                }

                settingsChecked++;
            }
        }

        Assert.True(settingsChecked > 0, $"{settingsChecked} > 0");
    }
}
