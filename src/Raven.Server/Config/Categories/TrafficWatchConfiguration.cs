using System.Collections.Generic;
using System.ComponentModel;
using Raven.Client.Documents.Changes;
using Raven.Client.ServerWide.Operations.TrafficWatch;
using Raven.Server.Config.Attributes;
using Sparrow;
using Size = Sparrow.Size;

namespace Raven.Server.Config.Categories;

[ConfigurationCategory(ConfigurationCategoryType.TrafficWatch)]
public class TrafficWatchConfiguration : ConfigurationCategory
{
    [Description("Traffic Watch logging mode.")]
    [DefaultValue(TrafficWatchMode.Off)]
    [ConfigurationEntry("TrafficWatch.Mode", ConfigurationEntryScope.ServerWideOnly)]
    public TrafficWatchMode TrafficWatchMode { get; set; }

    [Description("A semicolon-separated list of database names by which the Traffic Watch logging entities will be filtered. If not specified, Traffic Watch entities of all databases will be included. Example list: \"test-database;another-database;the-third-database\".")]
    [DefaultValue(null)]
    [ConfigurationEntry("TrafficWatch.Databases", ConfigurationEntryScope.ServerWideOnly)]
    public HashSet<string> Databases { get; set; }

    [Description("A semicolon-separated list of response status codes by which the Traffic Watch logging entities will be filtered. If not specified, Traffic Watch entities with any response status code will be included. Example list: \"200;500;404\".")]
    [DefaultValue(null)]
    [ConfigurationEntry("TrafficWatch.StatusCodes", ConfigurationEntryScope.ServerWideOnly)]
    public HashSet<int> StatusCodes { get; set; }
    
    [Description("Minimum response size by which the Traffic Watch logging entities will be filtered.")]
    [DefaultValue(0)]
    [MinValue(0)]
    [SizeUnit(SizeUnit.Bytes)]
    [ConfigurationEntry("TrafficWatch.MinimumResponseSize", ConfigurationEntryScope.ServerWideOnly)]
    public Size MinimumResponseSize { get; set; }

    [Description("Minimum request size by which the Traffic Watch logging entities will be filtered.")]
    [DefaultValue(0)]
    [MinValue(0)]
    [SizeUnit(SizeUnit.Bytes)]
    [ConfigurationEntry("TrafficWatch.MinimumRequestSize", ConfigurationEntryScope.ServerWideOnly)]
    public Size MinimumRequestSize { get; set; }
    
    [Description("Minimum duration by which the Traffic Watch logging entities will be filtered.")]
    [DefaultValue(0)]
    [ConfigurationEntry("TrafficWatch.MinimumDuration", ConfigurationEntryScope.ServerWideOnly)]
    public long MinimumDuration { get; set; }

    [Description("A semicolon-separated list of request HTTP methods by which the Traffic Watch logging entities will be filtered. If not specified, Traffic Watch entities with any HTTP request method will be included. Example list: \"GET;POST\".")]
    [DefaultValue(null)]
    [ConfigurationEntry("TrafficWatch.HttpMethods", ConfigurationEntryScope.ServerWideOnly)]
    public HashSet<string> HttpMethods { get; set; }

    [Description("A semicolon-separated list of Traffic Watch change types by which the Traffic Watch logging entities will be filtered. If not specified, Traffic Watch entities with any change type will be included. Example list: \"GET;POST\".")]
    [DefaultValue(null)]
    [ConfigurationEntry("TrafficWatch.ChangeTypes", ConfigurationEntryScope.ServerWideOnly)]
    public HashSet<TrafficWatchChangeType> ChangeTypes { get; set; }

}
