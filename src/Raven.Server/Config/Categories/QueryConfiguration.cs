using System.ComponentModel;
using Raven.Server.Config.Attributes;

namespace Raven.Server.Config.Categories
{
    public class QueryConfiguration : ConfigurationCategory
    {
        [DefaultValue(1024)] //1024 is Lucene.net default - so if the setting is not set it will be the same as not touching Lucene's settings at all
        [ConfigurationEntry("Raven/Query/MaxClauseCount")]
        [ConfigurationEntry("Raven/MaxClauseCount")]
        public int MaxClauseCount { get; set; }
    }
}