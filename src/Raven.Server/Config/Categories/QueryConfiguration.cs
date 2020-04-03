using System.ComponentModel;
using Raven.Server.Config.Attributes;

namespace Raven.Server.Config.Categories
{
    [ConfigurationCategory(ConfigurationCategoryType.Query)]
    public class QueryConfiguration : ConfigurationCategory
    {
        [DefaultValue(null)]//1024 is Lucene.net default, but we don't set it by default
        [ConfigurationEntry("Query.MaxClauseCount", ConfigurationEntryScope.ServerWideOnly)]
        public int? MaxClauseCount { get; set; }
    }
}
