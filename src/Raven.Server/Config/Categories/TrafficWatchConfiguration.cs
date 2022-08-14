using System.Collections.Generic;
using System.ComponentModel;
using Raven.Client.Documents.Changes;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Sparrow;
using Sparrow.Logging;

namespace Raven.Server.Config.Categories;

[ConfigurationCategory(ConfigurationCategoryType.TrafficWatch)]
public class TrafficWatchConfiguration : ConfigurationCategory
{
    [DefaultValue(TrafficWatchMode.Off)]
    [ConfigurationEntry("TrafficWatch.Mode", ConfigurationEntryScope.ServerWideOnly)]
    public TrafficWatchMode TrafficWatchMode { get; set; }

    [DefaultValue(null)]
    [ConfigurationEntry("TrafficWatch.Databases", ConfigurationEntryScope.ServerWideOnly)]
    public HashSet<string> Databases { get; set; }

    [DefaultValue(null)]
    [ConfigurationEntry("TrafficWatch.StatusCodes", ConfigurationEntryScope.ServerWideOnly)]
    public HashSet<int?> StatusCodes { get; set; }
    
    [DefaultValue(0)]
    [MinValue(0)]
    [SizeUnit(SizeUnit.Bytes)]
    [ConfigurationEntry("TrafficWatch.MinimumResponseSize", ConfigurationEntryScope.ServerWideOnly)]
    public Size MinimumResponseSize { get; set; }

    [DefaultValue(0)]
    [MinValue(0)]
    [SizeUnit(SizeUnit.Bytes)]
    [ConfigurationEntry("TrafficWatch.MinimumRequestSize", ConfigurationEntryScope.ServerWideOnly)]
    public Size MinimumRequestSize { get; set; }
    
    [DefaultValue(0)]
    [ConfigurationEntry("TrafficWatch.MinimumDuration", ConfigurationEntryScope.ServerWideOnly)]
    public long MinimumDuration { get; set; }

    [DefaultValue(null)]
    [ConfigurationEntry("TrafficWatch.HttpMethods", ConfigurationEntryScope.ServerWideOnly)]
    public HashSet<string> HttpMethods { get; set; }

    [DefaultValue(null)]
    [ConfigurationEntry("TrafficWatch.ChangeTypes", ConfigurationEntryScope.ServerWideOnly)]
    public HashSet<TrafficWatchChangeType> ChangeTypes { get; set; }

    [DefaultValue(128)]
    [MinValue(16)]
    [SizeUnit(SizeUnit.Megabytes)]
    [ConfigurationEntry("TrafficWatch.MaxFileSizeInMb", ConfigurationEntryScope.ServerWideOnly)]
    public Size MaxFileSize { get; set; }

    [Description("The maximum size of the log after which the old files will be deleted")]
    [DefaultValue(null)]
    [MinValue(256)]
    [SizeUnit(SizeUnit.Megabytes)]
    [ConfigurationEntry("TrafficWatch.RetentionSizeInMb", ConfigurationEntryScope.ServerWideOnly)]
    public Size RetentionSize { get; set; }

    [Description("Will determine whether to compress the log files")]
    [DefaultValue(false)]
    [ConfigurationEntry("TrafficWatch.Compress", ConfigurationEntryScope.ServerWideOnly)]
    public bool Compress { get; set; }

    [DefaultValue("Logs")]
    [ConfigurationEntry("TrafficWatch.Path", ConfigurationEntryScope.ServerWideOnly)]
    public PathSetting Path { get; set; }
}
