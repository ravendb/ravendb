using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Indexes.Configuration;

namespace Raven.Server.Config.Categories
{
    [ConfigurationCategory(ConfigurationCategoryType.Query)]
    public sealed class QueryConfiguration : ConfigurationCategory
    {
        [DefaultValue(null)]//1024 is Lucene.net default, but we don't set it by default
        [ConfigurationEntry("Query.MaxClauseCount", ConfigurationEntryScope.ServerWideOnly)]
        public int? MaxClauseCount { get; set; }
        
        [Description("Timeout for Regex in regex query.")]
        [TimeUnit(TimeUnit.Milliseconds)]
        [DefaultValue(100)]
        [IndexUpdateType(IndexUpdateType.Refresh)]
        [ConfigurationEntry("Query.RegexTimeoutInMs", ConfigurationEntryScope.ServerWideOrPerDatabaseOrPerIndex)]
        public TimeSetting RegexTimeout { get; set; }
    }
}
